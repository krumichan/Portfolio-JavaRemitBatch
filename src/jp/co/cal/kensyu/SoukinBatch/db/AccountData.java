/**
 * 研修プログラム：送金一括処理
 * 
 * dto:口座テーブル
 * 
 * Create 2017/11/20
 * @author CAL
 */
package jp.co.cal.kensyu.SoukinBatch.db;

/*
 * クラス名：AccountData
 * 機能：口座テーブル [T_Account]のデータクラス
 * */
public class AccountData {
	/* フィールド定義 */
	private short pno;		//口座No
	private String pname;	//口座名義人氏名
	private int zandaka;		//残高

	public short getPno() {
		return pno;
	}
	public void setPno(short pno) {
		this.pno = pno;
	}
	
	public String getPname() {
		return pname;
	}
	public void setPname(String pname) {
		this.pname = pname;
	}
	
	public int getZandaka() {
		return zandaka;
	}
	public void setZandaka(int zandaka) {
		this.zandaka = zandaka;
	}
	
}