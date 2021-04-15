/**
 * 研修プログラム：送金一括処理
 * 
 * dao:口座テーブル [T_Account]
 * 
 * Create 2017/11/20
 * @author CAL
 */
package jp.co.cal.kensyu.SoukinBatch.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.lang.StringBuilder;
import java.util.HashMap;
import java.util.Map;
import java.util.Date;
import java.util.List;
import java.util.ArrayList;
import jp.co.cal.kensyu.SoukinBatch.AppException;

/*
 * クラス名：AccountDao
 * 機能：口座テーブル [T_Account]の操作クラス
 * 
 * 使用方法：
 *  connect DBに接続する
 *  disConnect DB接続を切断する
 *  select, update 口座Noを指定して取得、更新する
 *  updateLock 口座Noを指定してレコードをロックする
 *               ※ロック状態を維持するため、Statement、ResultSetに名称を付けて内部保持する
 *               ※ロック状態を解放するため、commit, rollbackを実行する
 *  commit, rollback update結果を確定、破棄する
 * */
public class AccountDao {
	/* DB接続定義 */
	private static final String user="testuser";
	private static final String password="testpass";
	private static final String dbserver="192.168.0.170:1521";
	private static final String dbname="testdb";
	
	/* ロックの種類 */
	public static final byte LockMode_NoLock=0;	//ロックしない
	public static final byte LockMode_Wait=1;		//待機
	public static final byte LockMode_NoWait=2;	//待機せずエラーにする
	
	/* フィールド定義 */
	private Connection _conn=null; 
	private Map<String, Statement> _lockStmtList=new HashMap<String, Statement>(); //ロック保持のため
	private Map<String, ResultSet> _lockRsetList=new HashMap<String, ResultSet>(); //ロック保持のため
	
	/*
	 * Connectionを生成する
	 */
	public void connect() throws Exception {
		
		/* ドライバクラスのロード */
		Class.forName("oracle.jdbc.driver.OracleDriver");
		
		/* Connectionの生成 */
		_conn=DriverManager.getConnection("jdbc:oracle:thin:@" + dbserver + ":" + dbname, user, password);
		
		/* 手動Commit,Rollbackモードにする */
		_conn.setAutoCommit(false);
	}
	
	/*
	 * Connectionを閉じる
	 * */
	public void disConnect() {
		
		try {
			for(String keyName : _lockRsetList.keySet()) {
				this.closeLockRset(keyName);
			}
			
			for(String keyName : _lockStmtList.keySet()) {
				this.closeLockStmt(keyName);
			}
			
			_conn.close();
		} catch (Exception e) {
			//nop
		}
		
	}
	
	/*
	 * 口座番号を指定し口座テーブルから口座情報を取得する
	 * 参照モード専用
	 * 
	 * 引数：
	 * 	pno		口座番号
	 * 戻り値：
	 *	取得した口座情報をデータクラスで返す
	 * */
	public AccountData select(short pno) throws Exception {
		Statement stmt=null;
		ResultSet rset=null;
		AccountData account=null;
		StringBuilder sql=new StringBuilder();
		
		try {
			stmt=_conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			
			sql.append("SELECT pno");
			sql.append(",pname");
			sql.append(",zandaka");
			sql.append(" FROM T_ACCOUNT");
			sql.append(" WHERE");
			sql.append(" pno=" + String.valueOf(pno) );
			
			rset=stmt.executeQuery(sql.toString());
			
			if (rset.next()) {
				account=new AccountData();
				account.setPno(rset.getShort("pno"));
				account.setPname(rset.getString("pname"));
				account.setZandaka(rset.getInt("zandaka"));
			}
			
		} catch (SQLException e) {
			AppException.putMsg(sql.toString());
			throw e;
		} catch (Exception e) {
			throw e;
		} finally {
			if (rset != null) {
				rset.close();
			}
			if (stmt != null) {
				stmt.close();
			}
		}
		
		return account;
	}

	/*
	 * 口座テーブルから本処理で更新があった口座情報を取得する
	 * 参照モード専用
	 * 
	 * 引数：
	 * 	batchTime	バッチ起動日時
	 * 戻り値：
	 *	取得した口座情報をデータクラスのlistで返す
	 * */
	public List<AccountData> selectCurrentUpdate(Date batchTime) throws Exception {
		List<AccountData> listAccDat =new ArrayList<AccountData>();
		Statement stmt=null;
		ResultSet rset=null;
		AccountData account=null;
		StringBuilder sql=new StringBuilder();
		
		try {
			stmt=_conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			
			sql.append("SELECT pno");
			sql.append(",pname");
			sql.append(",zandaka");
			sql.append(" FROM T_ACCOUNT");
			sql.append(" WHERE");
			sql.append(" batchupdate=" + getSqlDateTime(batchTime) );
			sql.append(" ORDER BY pno");
			
			rset=stmt.executeQuery(sql.toString());
			
			while (rset.next()) {
				account=new AccountData();
				account.setPno(rset.getShort("pno"));
				account.setPname(rset.getString("pname"));
				account.setZandaka(rset.getInt("zandaka"));

				listAccDat.add(account);
			}
			
			
		} catch (SQLException e) {
			AppException.putMsg(sql.toString());
			throw e;
		} catch (Exception e) {
			throw e;
		} finally {
			if (rset != null) {
				rset.close();
			}
			if (stmt != null) {
				stmt.close();
			}
		}
		
		return listAccDat;
	}	
	/*
	 * 口座番号を指定し口座テーブルの残高を更新する
	 * ※commit, rollbackで確定が必要
	 * 
	 * 引数：
	 * 	pno			口座番号
	 *	zandaka	更新する新しい残高値
	 * */
	public long update(short pno, int zandaka, Date batchTime) throws Exception {
		long retUpdateCnt=0;
		Statement stmt=null;
		StringBuilder sql=new StringBuilder();
		
		try {
			stmt = _conn.createStatement();
			
			sql.append("UPDATE T_ACCOUNT");
			sql.append(" SET");
			sql.append(" zandaka=" + String.valueOf(zandaka));
			sql.append(",batchupdate=" + getSqlDateTime(batchTime) );
			sql.append(" WHERE");
			sql.append(" pno=" + String.valueOf(pno) );
			
			retUpdateCnt = stmt.executeUpdate(sql.toString());
			
		} catch (SQLException e) {
			//SQL構文エラー
			AppException.putMsg(sql.toString());	//SQL文字列を出力
			throw e;
			
		} catch (Exception e) {
			throw e;
			
		} finally {
			if (stmt != null) {
				stmt.close();
			}
		}
		
		return retUpdateCnt;
	}
	
	/*
	 * 口座番号を指定し口座テーブルの該当レコードをロックする
	 * 処理完了後、closeLockRsetとcloseLockStmt を実行し
	 *          更に commit, rollback どちらかを実行すること
	 * 
	 * 引数：
	 * keyName		管理名称 
	 * 				※使用したresultset, Statementを解放したり別のクエリを実行するとロックが解除される
	 * 				  そのため、同時に複数レコードをロックする場合は違う管理名称を使用する
	 *				※重要※ ロック解除は commit,rollbackすること ※重要※
	 * 	pno			口座番号
	 *	lockMode	ロックモード lockMode_*
	 * */
	public AccountData updateLock(String keyName, short pno, byte lockMode) throws Exception {
		AccountData account=null;
		
		try {
			//使用されている管理名称であればロック解除して解放する
			this.closeLockRset(keyName);
			this.closeLockStmt(keyName);
			
			//ロック維持するためstatementを作成し、内部listに保持する
			_lockStmtList.put(keyName, _conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE));
			
			StringBuilder sql=new StringBuilder();
			sql.append("SELECT pno");
			sql.append(",pname");
			sql.append(",zandaka");
			sql.append(" FROM T_ACCOUNT");
			sql.append(" WHERE");
			sql.append(" pno=" + String.valueOf(pno) );
			
			switch (lockMode) {
			case LockMode_Wait:
				sql.append(" FOR UPDATE WAIT");
				break;
			case LockMode_NoWait:
				sql.append(" FOR UPDATE NOWAIT");
				break;
			default:
			}
			
			//ロック維持するためresultsetを作成し、内部listに保持する
			_lockRsetList.put(keyName,_lockStmtList.get(keyName).executeQuery(sql.toString()));

			if (_lockRsetList.get(keyName).next()) {
				account=new AccountData();
				account.setPno(_lockRsetList.get(keyName).getShort("pno"));
				account.setPname(_lockRsetList.get(keyName).getString("pname"));
				account.setZandaka(_lockRsetList.get(keyName).getInt("zandaka"));
			} else {
				throw new Exception("該当するレコードは存在しません。SQL:" + sql.toString());
			}
			
		} catch ( SQLException e ) {
			this.closeLockRset(keyName);
			this.closeLockStmt(keyName);

			throw e;
			
		} catch ( Exception e ) {
			this.closeLockRset(keyName);
			this.closeLockStmt(keyName);
			
			throw e;
		}		
		
		return account;
	}		

	/*
	 * updateLockのロックを解除する
	 * 
	 * 引数
	 * 	キー名（ロックした後別のクエリを実行するとロックが消えるので名称付けて管理）
	 * */
	public void releaseLock(String keyName) throws Exception {
		this.closeLockRset(keyName);
		this.closeLockStmt(keyName);
	}
	
	/*
	 * Commit
	 * */
	public void commit() throws Exception {
		_conn.commit();
	}
	
	/*
	 * Rollback
	 * */
	public void rollback() {
		try {
			_conn.rollback();
		} catch (Exception e) {
			//nop
		}
	}

	/*
	 * DBサーバからシステム日付を取得する
	 * 
	 * 戻り値：
	 * 	日付
	 * */
	public Date getSysDateTime() throws Exception {
		Statement stmt=null;
		ResultSet rset=null;
		Date nowtime=null;
		
		try {
			stmt=_conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			
			rset=stmt.executeQuery("SELECT SYSDATE FROM DUAL");
			
			if (rset.next()) {
				nowtime=rset.getDate(1);
			}
			
		} catch (SQLException e) {
			throw e;
		} catch (Exception e) {
			throw (new AppException(e));
		} finally {
			if (rset != null) {
				rset.close();
			}
			if (stmt != null) {
				stmt.close();
			}
		}
		
		return nowtime;		
	}
	
	
	/*
	 * updateLockのロックを解除する
	 * 
	 * 引数:
	 * 	name		キー名
	 * 				ロックした後別のクエリを実行するとロックが消えるので名称付けて管理
	 * */
	private void closeLockRset(String name) throws Exception {
		
		if (_lockRsetList.containsKey(name)) {
			
			if (!_lockRsetList.get(name).isClosed()) {
				_lockRsetList.get(name).close();
			}
			_lockRsetList.remove(name);
		}
	}
		
	/*
	 * updateLockのロックを解除する
	 * 
	 * 引数:
	 * 	name		キー名
	 * 				ロックした後別のクエリを実行するとロックが消えるので名称付けて管理
	 * */
	private void closeLockStmt(String name) throws Exception {		
		
		if (_lockStmtList.containsKey(name)) {
			
			if (!_lockStmtList.get(name).isClosed()) {
				_lockStmtList.get(name).close();
			}
			_lockStmtList.remove(name);
		}
	}

	/*
	 * 日時をSQL用に文字列変換する
	 * 
	 * 引数：
	 * 	dt		日付
	 * */
	private String getSqlDateTime(Date dt)
	{
		//java date->string
		SimpleDateFormat fmt=new java.text.SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		//oracle string->date
		String strTime = "to_date('" + fmt.format(dt) + "', 'YYYY/MM/DD HH24:MI:SS')";
		
		return strTime;
	}
	

}