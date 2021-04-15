/**
 * 研修プログラム：送金一括処理
 * 
 * CsvReader
 * 
 * "torihiki.csv"を読み込み
 * 
 * Create 2017/11/20
 * @author CAL
 */
package jp.co.cal.kensyu.SoukinBatch.file;

import java.util.List;
import java.util.ArrayList;
import java.io.File;
import java.io.FileReader;
import java.io.BufferedReader;
//import java.io.IOException;


import jp.co.cal.kensyu.SoukinBatch.AppException;
import jp.co.cal.kensyu.SoukinBatch.Soukin;

/*
 * クラス名：OrderCsvReader
 * 機能：送金データCSVの読み込み処理クラス
 * 
 * 使用方法：
 *  read 実行フォルダ直下のファイルを読み込んで List<String[]> で取得する
 * */
public class OrderCsvReader {
	/* 定数 */
	public static final String CSV_FILENAME="torihiki.csv";	//読み込みCSVファイル名

	public static final int FLD_DATE=0;		//CSV Fld1 送金処理日 yyyy/mm/dd
	public static final int FLD_PNOSEND=1;		//CSV Fld2 送金者 口座番号
	public static final int FLD_PNORECV=2;		//CSV Fld3 振込先 口座番号
	public static final int FLD_MONEY=3;		//CSV Fld4 送金金額
	public static final int FLD_ROWNO=4;		//後付け Fld5 行番号
	
	private static final int FLDS_CNT=5;	//CSV項目数+1(行番号)

	/* フィールド定義 */

	private String filePath = null;	//読み込みファイルのフルパス
	
	private int totalRowCount=0;	//読み込み行数
	private int emptyRowCount=0;	//読み込み行数のうち、データ無し行数
	private int errorRowCount=0;	//読み込み行数のうち、項目数や型不一致などエラー行数
	
	
	private void setFilePath(String filePath) {
		this.filePath = filePath;
	}

	public String getFilePath() {
		return filePath;
	}

	public int getTotalRowCount() {
		return totalRowCount;
	}

	public int getEmptyRowCount() {
		return emptyRowCount;
	}

	public int getErrorRowCount() {
		return errorRowCount;
	}

	/* CSVファイル読み込み
	 * 項目毎の型チェック、最大値チェックをおこないエラー行はスキップする
	 * 
	 * 引数
	 *  読み込むファイルパス
	 * 戻り値 
	 *  項目ごとに配列化した行データを、listで集約し返却する
	 *  */
	public List<String[]>  read(String filePath) throws Exception {
		List<String[]> orders = new ArrayList<String[]>();

		this.setFilePath(filePath);

		File file=new File(filePath);
		if (!file.exists()) {
			throw new AppException(AppException.APPERRCD_CSV_NOTFOUND);
		}
		
		try {
			BufferedReader br = new BufferedReader(new FileReader(file));
			
			try {
				totalRowCount=0;
				String line = null;				
				
				while ((line=br.readLine()) != null) {
					totalRowCount++;
					String trmLine="";
					try {
						trmLine=line.replaceAll("^[\\s　]*", "").replaceAll("[\\s　]*$", "");
						String fldDats[] = splitCsvRow(totalRowCount, trmLine);
						
						orders.add(fldDats);
						
					} catch (AppException e) {
						if (e.getErrCode()==AppException.APPERRCD_CSV_EMPTYROW) {
							emptyRowCount++;
							AppException.putLineMsg(totalRowCount, "空行", e.getErrCode(), trmLine);
						} else {
							errorRowCount++;
							AppException.putLineMsg(totalRowCount, "失敗", e, trmLine);
						}
						
						//next row
					} catch (Exception e) {
						errorRowCount++;
						AppException appEx = new AppException(e);
						AppException.putLineMsg(totalRowCount, "失敗", appEx, trmLine);
						throw appEx;	//exit
					}
				}
			} finally {
				br.close();
			}
			
		} catch (AppException e) {
			throw e;
		}
		
		return orders;
	}
	
	/* 行バッファデータを配列化して返却する
	 * 項目ごとのチェック
	 * 
	 * 引数
	 *  読み込んだ行番号
	 *  読み込んだ行データ
	 * 戻り値
	 *  作成された配列  ※エラーは例外を発生させる  
	 *  */
	private String[] splitCsvRow(int rowCnt, String line) throws Exception {
		if (line.length()==0) {			
			throw new AppException(AppException.APPERRCD_CSV_EMPTYROW); 
		}
		
		String fldDats[] = ( line + "," + String.valueOf(rowCnt)).split("," ,-1);
		if (fldDats.length!=FLDS_CNT) {
			throw new AppException(AppException.APPERRCD_CSV_FIELDUNMATCH); 
		}
		
		if ( !IsDateFormat(fldDats[FLD_DATE]) ) {
			throw new AppException(AppException.APPERRCD_CSV_FLD1); 
		}
		
		if ( !isNumber(fldDats[FLD_PNOSEND], 1, Soukin.MAX_PNO) ) {
			throw new AppException(AppException.APPERRCD_CSV_FLD2);
		}

		if ( !isNumber(fldDats[FLD_PNORECV], 1, Soukin.MAX_PNO) ) {
			throw new AppException(AppException.APPERRCD_CSV_FLD3);
		}

		if ( !isNumber(fldDats[FLD_MONEY], 1, Soukin.MAX_ZANDAKA) ) {
			throw new AppException(AppException.APPERRCD_CSV_FLD4);
		}
		
		return fldDats;
	}
	
	/*
	 * データが日付形式か調べる
	 * 
	 * 引数
	 *  調べるデータ
	 * 戻り値
	 *  true/false
	 * */
	private boolean IsDateFormat( String strDateVal ) {
		boolean isDate = false;
		
		java.text.DateFormat fmt = new java.text.SimpleDateFormat("yyyy/MM/dd");
		fmt.setLenient(false);
		try {
			fmt.parse(strDateVal);
			isDate = true;
		} catch (java.text.ParseException e) {
			//何もしない
		}
		
		return isDate;
	} 

	/*
	 * データが整数か調べる
	 * 
	 * 引数
	 *  調べるデータ
	 *  最小値
	 *  最大値
	 * 戻り値
	 *  true/false
	 * */
	private boolean isNumber(String val, int minNum, int maxNum) {
		boolean isNum = false;
		
		try {
			int i=Integer.parseInt(val);
			if (i>=minNum && i<=maxNum) {
				isNum=true;
			}
		} catch (NumberFormatException e) {
			//何もしない
		}
		return isNum;
	}

	/*
	 * 実行されているjarファイルのフォルダパスを取得する
	 * 
	 * 戻り値
	 *  フォルダパス
	 * */
	public static String getCurrentFolderPath() {
		String jarPath=System.getProperty("java.class.path");
		
		int pathPos;
		String folPath;
		if ((pathPos=jarPath.indexOf(System.getProperty("path.separator")))<0) {
			folPath=jarPath;
		} else {
			folPath=jarPath.substring(0, pathPos);
		}
		
		if (new File(folPath).isFile()) {
			folPath = (new File(folPath)).getParent();
		}
		
		String folderSp = System.getProperty("file.separator");
		if ( folPath.substring(folPath.length()-folderSp.length()) != folderSp) {
			folPath += folderSp;
		}
		
		return folPath;
	}
	
	/*
	 * 読み込むCSVファイルのフルパスを取得
	 * */
	public static String getInputCsvFilePath() {
		return getCurrentFolderPath() + CSV_FILENAME;		
	}

}

