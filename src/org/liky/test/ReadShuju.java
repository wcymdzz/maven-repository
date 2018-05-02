package org.liky.test;



import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.MapFile.Reader;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;



public class ReadShuju {

	private static Configuration conf = new Configuration();
	private static Path path = new Path("hdfs://10.60.72.207:9000/news_index_output");
	private static FileSystem fs;
	private static Reader reader;
	static {
		try {
			fs = path.getFileSystem(conf);
			reader = MapFileOutputFormat.getReaders(path, conf)[0];
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static String findKeyword(String keyword) throws Exception {
		return reader.get(new Text(keyword), new Text()).toString();
	}
	

}
