package org.liky.test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;

import jeasy.analysis.MMAnalyzer;

public class TestMR {

	public static class FirstMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		private static MMAnalyzer mm = new MMAnalyzer();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// ��Ҫ�������ִʲ���
			String[] results = value.toString().split("\t");
			// ��title���շִʹ�����в��
			if (results.length > 1) {
				String[] allTitleWords = mm.segment(results[1], "|").split("\\|");
				for (String word : allTitleWords) {
					context.write(new Text(word), new IntWritable(Integer.parseInt(results[0])));
				}
			}
		}
	}

	public static class SecondMapper extends Mapper<Text, IntWritable, Text, Text> {

		public void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {
			// ��ƴ��һ�� :1
			context.write(key, new Text(value + ":1"));
		}
	}

	public static class MyReducer extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			StringBuilder builder = new StringBuilder();
			// key��id,value�Ǵ���
			Map<Integer, Integer> map = new HashMap<Integer, Integer>();
			// ѭ������value
			for (Text value : values) {
				// �Ȱ�","���,��ֺ�ÿһ���ͱ�ʾ1��id��Ӧ������
				String[] idStrs = value.toString().split(",");
				for (String idStr : idStrs) {
					String[] idCountValue = idStr.split(":");
					if (idCountValue.length > 1) {
						Integer id = Integer.parseInt(idCountValue[0]);
						Integer count = Integer.parseInt(idCountValue[1]);
						if (map.containsKey(id)) {
							// ����������Ѿ��й����id������Ҫ�������
							map.put(id, map.get(id) + count);
						} else {
							// ���������û��������ݣ�ֱ�ӷ��뼴��
							map.put(id, count);
						}
					}
				}
			}
			// ��map�е���������ѭ���������������ǵĹ������������
			Set<Integer> keySet = map.keySet();
			for (Integer id : keySet) {
				builder.append(id + ":" + map.get(id) + ",");
			}

			// ����������ȡ�����һ��������Ķ���
			context.write(key, new Text(builder.substring(0, builder.length() - 1)));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "click count sort");
		job.setJarByClass(TestMR.class);

		// ������ʽMapReduce
		ChainMapper.addMapper(job, FirstMapper.class, LongWritable.class, Text.class, Text.class, IntWritable.class,
				conf);
		ChainMapper.addMapper(job, SecondMapper.class, Text.class, IntWritable.class, Text.class, Text.class, conf);
		ChainReducer.setReducer(job, MyReducer.class, Text.class, Text.class, Text.class, Text.class, conf);

		// ֻ�������Reducer��������ͼ��ɡ�
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		// ������������key-value��map��ʽ�����
		job.setOutputFormatClass(MapFileOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path("hdfs://10.60.72.207:9000/news_index_input"));
		Path outputPath = new Path("hdfs://10.60.72.207:9000/news_index_output");
		FileSystem fs = outputPath.getFileSystem(conf);
		if (fs.exists(outputPath)) {
			fs.delete(outputPath, true);
		}
		FileOutputFormat.setOutputPath(job, outputPath);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
