package org.liky.test;

import java.util.List;

import org.liky.dao.TestDAO;
import org.liky.dbc.DataBaseConnection;
import org.liky.factory.TestFactory;
import org.liky.vo.TestVo;

public class TestFind {

	public static void main(String[] args) throws Exception {
		DataBaseConnection dbc = new DataBaseConnection();
		TestDAO dao = TestFactory.getTestDAOInstance(dbc);
		List<TestVo> allData = dao.findByKeyword("����");
		System.out.println(allData);
		long start = System.currentTimeMillis();
		allData = dao.findByKeyword("����");
		long end = System.currentTimeMillis();
		System.out.println("��ʱ:" + (end - start) + " ms");
		System.out.println(allData);
	}

}
