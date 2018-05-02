package org.liky.dao.impl;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import org.liky.dao.TestDAO;
import org.liky.dbc.DataBaseConnection;
import org.liky.test.ReadShuju;
import org.liky.vo.TestVo;

import com.sun.org.apache.bcel.internal.generic.ReturnaddressType;

public class TestDAOImpl implements TestDAO {
	private DataBaseConnection dbc;
	

	public TestDAOImpl(DataBaseConnection dbc) {
		super();
		this.dbc = dbc;
	}

	@Override
	public List<TestVo> findAll() throws Exception {
		String sql="SELECT id,title FROM news";
		PreparedStatement pst=dbc.getConnection().prepareStatement(sql);
		ResultSet rs=pst.executeQuery();
		List<TestVo>allDate=new ArrayList<TestVo>();
		while(rs.next()){
			int id=rs.getInt(1);
			String title=rs.getString(2);
			TestVo testVo=new TestVo(id,title,null);
			allDate.add(testVo);
		}
		rs.close();
		pst.close();
		dbc.close();
		return allDate;
	}

	@Override
	public List<TestVo> findByKeyword(String keyword) throws Exception {
		String temp = ReadShuju.findKeyword(keyword);
		// 拆分,并取得所有的id
		List<Integer> allIds = new ArrayList<Integer>();

		String sql = "SELECT id,title,url FROM news WHERE id IN (";

		String[] strs = temp.split(",");
		for (String str : strs) {
			allIds.add(Integer.parseInt(str.split(":")[0]));
			sql += "?,";
		}
		sql = sql.substring(0, sql.length() - 1) + ")";
		System.out.println(sql);

		PreparedStatement pst = dbc.getConnection().prepareStatement(sql);
		for (int i = 0; i < allIds.size(); i++) {
			pst.setInt(i + 1, allIds.get(i));
		}

		ResultSet rs = pst.executeQuery();
		List<TestVo> allData = new ArrayList<TestVo>();
		while (rs.next()) {
			int id = rs.getInt(1);
			String title = rs.getString(2);
			String url = rs.getString(3);
			TestVo news = new TestVo(id, title, url);
			allData.add(news);
		}
		rs.close();
		pst.close();

		return allData;
	}

}
