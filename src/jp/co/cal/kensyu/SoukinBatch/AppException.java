/**
 * 研修プログラム：送金一括処理
 * 
 * AppException
 * 
 * 例外処理クラス
 * 
 * Create 2017/11/20
 * @author CAL
 */
package jp.co.cal.kensyu.SoukinBatch;

/*
 * クラス名：AppException
 * 機能：アプリ固有の例外
 * */
public class AppException extends Exception  {
	private static final long serialVersionUID=113L; 

	/*  定数 */
	public static final int APPERRCD_SUCCESS=0;
		
	public static final int APPERRCD_DUPEX=11;
	public static final int APPERRCD_CSV_NOTFOUND=12;
	public static final int APPERRCD_DBCONNECT=13;
	public static final int APPERRCD_CSV_EMPTYROW=30;
	public static final int APPERRCD_CSV_FIELDUNMATCH=31;
	public static final int APPERRCD_CSV_FLD1=32;
	public static final int APPERRCD_CSV_FLD2=33;
	public static final int APPERRCD_CSV_FLD3=34;
	public static final int APPERRCD_CSV_FLD4=35;
	public static final int APPERRCD_OLD_DATE=50;
	public static final int APPERRCD_NOPNO_SEND=51;
	public static final int APPERRCD_NOPNO_RECV=52;
	public static final int APPERRCD_SHORT_ZANDAKA=53;
	public static final int APPERRCD_OVERFLOW_ZANDAKA=54;
	public static final int APPERRCD_ERR_ZANDAKA=55;
	public static final int APPERRCD_FAIL_LOCK=56;
	
	public static final int APPERRCD_ERR_UNDEFINED=9999;
	
	public static final String APPERRMSG_UBNORMALEND="送金一括処理を中断しました。";
	public static final String APPERRMSG_NORMALEND="送金一括処理が終了しました。";

	/* フィールド定義 */
	private int errCode;
	private String errMsg="";

	public int getErrCode() {
		return errCode;
	}
		
	public String getErrMsg() {
		return errMsg;
	}

	/* コンストラクタ */
	public	AppException( int errCd ) {
		super();
		setErr(errCd);
	}

	public	AppException( int errCd, String addMsg ) {
		super();
		setErr(errCd);
		
		if (addMsg.length()>0) {
			errMsg += addMsg;
		}
	}	

	/*
	 * 一般例外を固有冷害に変換する
	 * */
	public AppException( Exception e ) {
		super(e);
		setErr(AppException.APPERRCD_ERR_UNDEFINED);
		
		errMsg = e.getMessage() + System.getProperty("line.separator") + e.getStackTrace();
	}
		
	/*
	 * エラーコードに対応するエラー文字列を取得する
	 * 
	 * 引数
	 *   エラーコード
	 * 戻り値
	 *   エラー文字列
	 * */
	public static String getAppErrMsg(int errCd ) {
		String msg="";
		
		switch (errCd) {
		case APPERRCD_DUPEX:
			msg = "この端末で、すでに送金一括処理が実行されています。";
			break;
		case APPERRCD_CSV_NOTFOUND:
			msg = "取引データファイルが見つかりませんでした。";
			break;
		case APPERRCD_DBCONNECT:
			msg = "データベースの接続に失敗しました。";
			break;
		case APPERRCD_CSV_EMPTYROW:
			msg = "空行です。";
			break;
		case APPERRCD_CSV_FIELDUNMATCH:
			msg = "データ項目数が一致しません。";
			break;
		case APPERRCD_CSV_FLD1:
			msg = "項目１が日付形式ではありません。";
			break;
		case APPERRCD_CSV_FLD2:
			msg = "項目２が 1〜99の整数ではありません。";
			break;
		case APPERRCD_CSV_FLD3:
			msg = "項目３が 1〜99の整数ではありません。";
			break;
		case APPERRCD_CSV_FLD4:
			msg = "項目４が 1〜99999999の整数ではありません。";
			break;
		case APPERRCD_OLD_DATE:
			msg = "過去日付の送金はできません。";
			break;
		case APPERRCD_NOPNO_SEND:
			msg = "送金者に指定された口座がありません。";
			break;
		case APPERRCD_NOPNO_RECV:
			msg = "振込先に指定された口座がありません。";
			break;
		case APPERRCD_SHORT_ZANDAKA:
			msg = "送金者の残高不足です。";
			break;
		case APPERRCD_OVERFLOW_ZANDAKA:
			msg = "振込先の残高がオーバーフローしました。";
			break;
		case APPERRCD_ERR_ZANDAKA:
			msg = "送金者または振込先の残高が不正です。";
			break;
		case APPERRCD_FAIL_LOCK:
			msg = "口座が別処理で使用中のため更新できませんでした。";
			break;

		default:
			msg = "未定義のエラー";
			break;
		}
		
		return msg;
	}
	
	/*
	 * エラー情報をセットする
	 * 
	 * 引数
	 *   エラーコード
	 * */
	private void setErr( int errCd ) {
		errCode=-1;
		errMsg="";

		switch (errCd) {
		case APPERRCD_DUPEX:
		case APPERRCD_CSV_NOTFOUND:
		case APPERRCD_DBCONNECT:
		case APPERRCD_CSV_EMPTYROW:
		case APPERRCD_CSV_FIELDUNMATCH:
		case APPERRCD_CSV_FLD1:
		case APPERRCD_CSV_FLD2:
		case APPERRCD_CSV_FLD3:
		case APPERRCD_CSV_FLD4:
		case APPERRCD_OLD_DATE:
		case APPERRCD_NOPNO_SEND:
		case APPERRCD_NOPNO_RECV:
		case APPERRCD_SHORT_ZANDAKA:
		case APPERRCD_OVERFLOW_ZANDAKA:
		case APPERRCD_ERR_ZANDAKA:
		case APPERRCD_FAIL_LOCK:
			errCode = errCd;
			errMsg = getAppErrMsg(errCd);
			break;

		default:
			errCode=APPERRCD_ERR_UNDEFINED;
			errMsg = getAppErrMsg(errCd);
			break;
		}
	}	
	
	/*
	 * 行ごとの処理結果ログを出力
	 * 
	 * 引数
	 *  行番号
	 *  結果 ｛成功, 失敗｝
	 *  エラーコード
	 *  入力された行データ 
	 * */
	public static void putLineMsg(int rowNo, String resCaption, int cd, String lineData) {
		String msg=String.format("%d行：%s(%d)：%s", rowNo, resCaption, cd, lineData);
		AppException.putMsg(msg);
	}

	/*
	 * 行ごとの処理結果ログを出力
	 * 
	 * 引数
	 *  行番号
	 *  結果 ｛成功, 失敗｝
	 *  固有エラーオブジェクト
	 *  入力された行データ 
	 * */
	public static void putLineMsg(int rowNo, String resCaption,AppException e, String lineData) {
		String msg=String.format("%d行：%s(%d)：%s %s", rowNo, resCaption, e.getErrCode(), e.getErrMsg(), lineData);
		AppException.putMsg(msg);
	}

	/*
	 * 処理全体の結果ログを出力
	 * 
	 * 引数
	 *  結果 ｛成功, 失敗｝
	 *  エラーコード
	 *  メッセージ 
	 * */
	public static void putAppMsg(String resCaption,int cd, String addMsg) {
		String msg=String.format("%s(%d)：%s", resCaption, cd, addMsg);
		AppException.putMsg(msg);
	}
		
	/*
	 * ログ出力
	 * 
	 * 引数
	 *  出力データ
	 * */
	public static void putMsg(String msg) {
		System.out.println(msg);
	}
	
}
