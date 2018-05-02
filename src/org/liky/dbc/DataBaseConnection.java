package org.liky.dbc;


import java.sql.DriverManager;


import com.mysql.jdbc.Connection;

public class DataBaseConnection {
	private static final String DBDRIVER="org.gjt.mm.mysql.Driver";
	private static final String DBURL="jdbc:mysql://localhost:3306/student";
	private static final String DBUSER="root";
	private static final String DBPASSWORD="3333";
	public Connection conn;
	public DataBaseConnection(){
		
	}

	public Connection getConnection() {
		try {
			// ������ڻ�û������,���Ǿ�Ҫ����һ���µ�����
			if (conn == null || conn.isClosed()) {
				Class.forName(DBDRIVER);
				conn = (Connection) DriverManager.getConnection(DBURL, DBUSER, DBPASSWORD);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		return conn;
	}

	public void close() {
		try {
			if (conn != null) {
				conn.close();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
