/**
 * 研修プログラム：送金一括処理
 * 
 * 業務ロジッククラス
 * 
 * Create 2017/11/20
 * @author CAL
 */

package jp.co.cal.kensyu.SoukinBatch;

import java.util.List;
import java.util.Date;
import java.text.SimpleDateFormat;
import java.io.File;
//import java.text.ParseException;
import jp.co.cal.kensyu.SoukinBatch.file.OrderCsvReader;
import jp.co.cal.kensyu.SoukinBatch.db.*;
import jp.co.cal.kensyu.SoukinBatch.AppException;

/*
 * クラス名：Soukin
 * 機能：送金一括処理の業務クラス
 * 
 * 使用方法：
 *  exec 送金一括処理を実行する
 * */
public class Soukin {

	/* 定数 */
	public static final short MAX_PNO = 99;			//口座番号の最大値
	public static final int MAX_ZANDAKA = 99999999;	//残高の最大値
	
	private static final String LOCKKEY_SEND = "SEND";	//送金者レコードのロック名
	private static final String LOCKKEY_RECV = "RECV";	//振込先レコードのロック名

	/* フィールド定義 */
	private Date _baseDay = null;

	private int _totalRowCount=0;
	private int _emptyRowCount=0;
	private int _errorRowCount=0;
	private int _successRowCount=0;
		
	
	public int getTotalRowCount() {
		return _totalRowCount;
	}

	public int getEmptyRowCount() {
		return _emptyRowCount;
	}

	public int getErrorRowCount() {
		return _errorRowCount;
	}

	public int getSuccessRowCount() {
		return _successRowCount;
	}

	/*
	 * 送金一括処理のメインメソッド
	 * 
	 * １)起動時チェック
	 * ２)CSVファイル読み込み
	 * ３)1行ずつDB反映
	 * 
	 * 戻り値
	 *   結果コード： 正常時＝AppException.APPERRCD_SUCCESS
	 * */
	public int exec() {
		int retCd;
		AccountDao accDao=null;
		
		//１)起動時チェック
		retCd = startChk();
		if (retCd!=AppException.APPERRCD_SUCCESS) {
			AppException.putAppMsg("失敗", retCd, AppException.getAppErrMsg(retCd));
			return retCd;	
		}		

		try {
			//２)CSVファイル読み込み
			List<String[]> torihikiList=null;
			OrderCsvReader torihikiRd=null;

			torihikiRd = new OrderCsvReader();
			torihikiList = torihikiRd.read(OrderCsvReader.getInputCsvFilePath());

			//CSVファイル読み込み結果の引き継ぎ
			_emptyRowCount=torihikiRd.getEmptyRowCount();
			_totalRowCount=torihikiRd.getTotalRowCount();
			_errorRowCount=torihikiRd.getErrorRowCount();
			_successRowCount=0;

			//３)1行ずつDB反映
			accDao = new AccountDao();
			accDao.connect();
			
			for (int i=0; i<torihikiList.size(); i++) {
				//Listから1行分のデータを取得
				String[] flds=torihikiList.get(i);
				
				//ログ出力用に、行番号と配列化前データを取得
				int rowNo=Integer.parseInt(flds[OrderCsvReader.FLD_ROWNO]);
				String rowDat=restoreLine(flds);

				try {
					//送金処理実行
					runSoukin(accDao, flds);
					
					//成功
					accDao.commit();
					
					_successRowCount++;
					AppException.putLineMsg(rowNo, "成功", 0, rowDat);
					
				} catch (AppException e) {
					//失敗時は、該当行の処理をキャンセルし次行へ
					accDao.rollback();
					
					_errorRowCount++;
					AppException.putLineMsg(rowNo, "失敗", e, rowDat);
					
				} catch (Exception e) {
					//失敗時は、該当行の処理をキャンセルし次行へ
					accDao.rollback();
					
					_errorRowCount++;
					AppException appEx=new AppException(e);
					AppException.putLineMsg(rowNo, "失敗", appEx, rowDat);
				}
			}

			//更新した口座リストを出力
			putCurrentUpdateLog(accDao);
			
			retCd = AppException.APPERRCD_SUCCESS;

		} catch ( AppException e ) {
			AppException.putAppMsg("失敗", e.getErrCode(), e.getErrMsg());
			retCd = e.getErrCode();
			
		} catch(Exception e) {
			AppException appEx=new AppException(e);
			AppException.putAppMsg("失敗", appEx.getErrCode(), appEx.getErrMsg());
			retCd = appEx.getErrCode();

		} finally {
			
			if (accDao!=null) {
				accDao.disConnect();
			}
		}
				
		return retCd;
	}
		
	/*
	 * 起動時チェック
	 * 
	 * 1）読み込みCSVファイルの存在チェック
	 * 2）DB接続チェック
	 * 
	 * 戻り値
	 *   結果コード： 正常時＝AppException.APPERRCD_SUCCESS
	 * */
	private int startChk() {		
		//1）読み込みCSVファイルの存在チェック
		if (!(new File(OrderCsvReader.getInputCsvFilePath())).exists()) {
			AppException.putMsg(OrderCsvReader.getInputCsvFilePath());
			return AppException.APPERRCD_CSV_NOTFOUND;
		}
		
		//2）DB接続チェック 
		//   ついでにDBサーバのシステム日付を取得し、送金処理日の有効判定における基準日とする
		_baseDay=null;
		AccountDao accDao= new AccountDao();
		try {
			accDao.connect();
			_baseDay=accDao.getSysDateTime();
			
		} catch (Exception e) {
			//nop
			e.printStackTrace();
		} finally {
			accDao.disConnect();
		}
		
		if (_baseDay==null) {
			return AppException.APPERRCD_DBCONNECT;
		}		
		
		return AppException.APPERRCD_SUCCESS;
	}
	
	/*
	 * 送金処理（1処理のみ）を実行する
	 * 
	 * 引数
	 *   口座テーブルDAO
	 *   読み込んだ送金処理データ（行）
	 * */
	private void runSoukin(AccountDao accDao, String[] flds) throws Exception {
		//読み込みデータを変数にセット
		Date cmdDay=cnvDayStr(flds[OrderCsvReader.FLD_DATE]);
		short pnoSend=Short.parseShort(flds[OrderCsvReader.FLD_PNOSEND]);
		short pnoRecv=Short.parseShort(flds[OrderCsvReader.FLD_PNORECV]);
		int sendMoney=Integer.parseInt(flds[OrderCsvReader.FLD_MONEY]);
		
		/* 事前チェック */
		
		//読み込みデータの送金日付が、過去日付であればエラー行とする
		if (cnvStrDay(_baseDay).compareTo(cnvStrDay(cmdDay))>0 ) {
			throw (new AppException(AppException.APPERRCD_OLD_DATE));
		}			
		
		//送金元、送金先の口座Noの存在チェック
		if (!isExistPno(accDao, pnoSend)) {
			throw (new AppException(AppException.APPERRCD_NOPNO_SEND));
		}
		if (!isExistPno(accDao, pnoRecv)) {
			throw (new AppException(AppException.APPERRCD_NOPNO_RECV));
		}
		
		/* 送金処理 */
		AccountData sendAcc=null;
		AccountData recvAcc=null;
		try {
			//悲観ロック（該当レコードが処理中であればエラーとする）
			sendAcc = accDao.updateLock(Soukin.LOCKKEY_SEND, pnoSend, AccountDao.LockMode_NoWait);
			recvAcc = accDao.updateLock(Soukin.LOCKKEY_RECV, pnoRecv, AccountDao.LockMode_NoWait);
			if (sendAcc==null || recvAcc==null) {
				throw (new AppException(AppException.APPERRCD_FAIL_LOCK));
			}
			
			//計算後の残高が範囲内かチェック
			int resultValidate = ChkZandaka(sendAcc, recvAcc, sendMoney);
			if (resultValidate!=AppException.APPERRCD_SUCCESS) {
				throw (new AppException(resultValidate));
			}
			
			//update
			accDao.update(pnoSend, sendAcc.getZandaka()-sendMoney, _baseDay);
			accDao.update(pnoRecv, recvAcc.getZandaka()+sendMoney, _baseDay);			
		} finally {
			
			//lock解除
			if (sendAcc!=null) {
				accDao.releaseLock(Soukin.LOCKKEY_SEND);
			}
			if (recvAcc!=null) {
				accDao.releaseLock(Soukin.LOCKKEY_RECV);
			}
		}
	}
	

	/*
	 * 残高の仮計算をおこない、結果が残高の許容範囲内か調べる
	 * 
	 * 引数
	 *   送金者の口座情報
	 *   振込先の口座情報
	 *   送金金額
	 * 戻り値
	 *   結果コード： 正常時＝AppException.APPERRCD_SUCCESS
	 * */
	private int ChkZandaka(AccountData sendAcc, AccountData recvAcc, int sendMoney) {
		
		if (sendAcc.getZandaka()<sendMoney) {
			return AppException.APPERRCD_SHORT_ZANDAKA;
		}
		
		if (recvAcc.getZandaka()+sendMoney>Soukin.MAX_ZANDAKA) {
			return AppException.APPERRCD_OVERFLOW_ZANDAKA;
		}
		//上記とは逆の最小値・最大値チェック（通常有り得ない。DBデータの不備を検出）
		if ((recvAcc.getZandaka()+sendMoney<0) || (sendAcc.getZandaka()-sendMoney>Soukin.MAX_ZANDAKA)) {
			return AppException.APPERRCD_ERR_ZANDAKA;	
		}
		
		return AppException.APPERRCD_SUCCESS;
	}
	
	/*
	 * 口座Noが口座テーブルに存在するか調べる
	 * 
	 * 引数
	 *   口座テーブルDAO
	 *   口座No
	 * 戻り値
	 *   true=存在する、false=存在しない  
	 * */
	private boolean isExistPno(AccountDao accDao, short pno) {
		boolean isExist=false;
		
		try {
			if (accDao.select(pno)!=null) {
				isExist=true;
			}
		} catch (Exception e) {
			//nop
		}
		
		return isExist;
	}
	
	/*
	 * 文字列を Date型に変換する
	 * 書式：yyyy/MM/dd
	 * 
	 * 引数
	 *   変換する文字列
	 * 戻り値  
	 *   日付
	 * */
	private Date cnvDayStr(String strDate) {
		Date date = null;
		
		try {
			SimpleDateFormat fmt=new java.text.SimpleDateFormat("yyyy/MM/dd");
			fmt.setLenient(false);
			date = fmt.parse(strDate);
		} catch (Exception e) {
			//nop
		}
		
		return date; 
	}

	/*
	 * Date型を 文字列に変換する
	 * 書式：yyyy/MM/dd
	 * 
	 * 引数
	 *   変換する日付
	 * 戻り値  
	 *   文字列
	 * */
	private String cnvStrDay(Date day) {
		String strDay=null;
		
		try {
			SimpleDateFormat fmt=new java.text.SimpleDateFormat("yyyy/MM/dd");
			strDay = fmt.format(day);
		} catch (Exception e) {
			//nop
		}
		
		return strDay; 
	}

	/*
	 * ログ出力用に、読み込み行データ配列を行データの書式に戻す
	 * 
	 * 引数
	 *   読み込み行データ配列
	 * 戻り値
	 *   行データ文字列
	 * */
	private String restoreLine(String[] flds) {
		return flds[OrderCsvReader.FLD_DATE] + 
				"," + flds[OrderCsvReader.FLD_PNOSEND] +
				"," + flds[OrderCsvReader.FLD_PNORECV] +
				"," + flds[OrderCsvReader.FLD_MONEY];
	}
	
	/*
	 * 更新された口座リストを出力
	 * 
	 * 引数
	 *   口座テーブルDAO
	 * */
	private void putCurrentUpdateLog(AccountDao accDao) throws Exception {
		List<AccountData> listAccDat=null;
		
		listAccDat = accDao.selectCurrentUpdate(_baseDay);
		
		listAccDat.forEach( accDat -> {
			String sLog = String.format("%d:%d:%s", accDat.getPno(), accDat.getZandaka(), accDat.getPname()); 
			AppException.putMsg( sLog );				
		});
	}
	
}
