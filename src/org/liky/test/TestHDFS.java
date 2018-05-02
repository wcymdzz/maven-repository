package org.liky.test;

import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.liky.dao.TestDAO;
import org.liky.dbc.DataBaseConnection;
import org.liky.factory.TestFactory;
import org.liky.vo.TestVo;




public class TestHDFS {

	public static void main(String[] args)throws Exception {
		Configuration conf = new Configuration();
		String rootPath = "hdfs://10.60.72.207:9000/news_index_input/";
		Path path = new Path(rootPath);
		FileSystem fs = path.getFileSystem(conf);

		DataBaseConnection dbc = new DataBaseConnection();

		TestDAO dao = TestFactory.getTestDAOInstance(dbc);

		List<TestVo> allData = dao.findAll();

		for (TestVo news : allData) {
			String fileName = System.currentTimeMillis() + ".txt";
			Path filePath = new Path(rootPath + fileName);
			FSDataOutputStream os = fs.create(filePath);
			PrintWriter writer = new PrintWriter(new OutputStreamWriter(os, "UTF-8"));
			writer.print(news.getId() + "\t" + news.getTitle());
			writer.close();
		}

		dbc.close();
	
	}

}
