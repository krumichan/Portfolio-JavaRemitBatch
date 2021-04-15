/**
 * 研修プログラム：送金一括処理
 * 
 * Create 2017/11/20
 * @author CAL
 */

package jp.co.cal.kensyu.SoukinBatch;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;

/*
 * クラス名：SoukinBatch
 * 機能：送金一括処理のブートクラス
 * 
 * プログラム起動で、void main(String[] args)が呼び出される
 * */
public class SoukinBatch {

	/*  定数 */
	private static final String LOCK_FILENAME = "SoukinBatch.lock";	//重複起動制御様のロックファイル

	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		 (new SoukinBatch()).start() ;
	}
	
	/*
	 *  メイン処理
	 * */
	private int exec() {
		int retCd = -1;
		
		//送金一括処理の実行
		Soukin soukin = new Soukin();
		retCd = soukin.exec();

		//実行結果の判定
		if (retCd==AppException.APPERRCD_SUCCESS) {
			//正常終了時の出力
			AppException.putMsg(AppException.APPERRMSG_NORMALEND);
			AppException.putMsg( String.format("（行数：%d行 成功：%d行 失敗：%d行 空行：%d行）"
					, soukin.getTotalRowCount()
					, soukin.getSuccessRowCount()
					, soukin.getErrorRowCount()
					, soukin.getEmptyRowCount()
					) );
		} else {
			//エラー時の出力
			AppException.putMsg(AppException.APPERRMSG_UBNORMALEND);
		}
		
		return retCd;
	}

	/*
	 *  重複起動チェック後、メイン処理を呼び出す
	 * */
	private int start() {
		int retCd = -1;
		
		FileOutputStream fos=null;
		FileLock lock=null;
		FileChannel fc=null;
		
		//ロックファイルによる重複起動チェックをおこない、メイン処理を呼び出す
		try {
			try {
				fos=new FileOutputStream(LOCK_FILENAME);
				fc=fos.getChannel();
				try {
					lock=fc.tryLock();
					if (lock != null) {
						//メイン処理の呼び出し
						retCd = this.exec();
						
					} else {
						//重複起動メッセージの出力
						System.out.println(AppException.getAppErrMsg(AppException.APPERRCD_DUPEX));

					}
				} finally {
					if (lock !=null) {
						lock.release();
					}
				}
			} finally {
				if ( fc != null) {
					fc.close();
				}
				if ( fos != null) {
					fos.close();
				}
			}			
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return retCd;
	}
}