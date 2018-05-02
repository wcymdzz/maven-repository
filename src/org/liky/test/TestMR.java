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
			// 需要先做个分词操作
			String[] results = value.toString().split("\t");
			// 将title按照分词规则进行拆分
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
			// 都拼上一个 :1
			context.write(key, new Text(value + ":1"));
		}
	}

	public static class MyReducer extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			StringBuilder builder = new StringBuilder();
			// key是id,value是次数
			Map<Integer, Integer> map = new HashMap<Integer, Integer>();
			// 循环所有value
			for (Text value : values) {
				// 先按","拆分,拆分后每一个就表示1个id对应的数据
				String[] idStrs = value.toString().split(",");
				for (String idStr : idStrs) {
					String[] idCountValue = idStr.split(":");
					if (idCountValue.length > 1) {
						Integer id = Integer.parseInt(idCountValue[0]);
						Integer count = Integer.parseInt(idCountValue[1]);
						if (map.containsKey(id)) {
							// 如果集合中已经有过这个id，则需要进行求和
							map.put(id, map.get(id) + count);
						} else {
							// 如果集合中没有这个数据，直接放入即可
							map.put(id, count);
						}
					}
				}
			}
			// 将map中的所有数据循环出来，按照我们的规则来进行输出
			Set<Integer> keySet = map.keySet();
			for (Integer id : keySet) {
				builder.append(id + ":" + map.get(id) + ",");
			}

			// 结果输出，截取掉最后一个多出来的逗号
			context.write(key, new Text(builder.substring(0, builder.length() - 1)));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "click count sort");
		job.setJarByClass(TestMR.class);

		// 设置链式MapReduce
		ChainMapper.addMapper(job, FirstMapper.class, LongWritable.class, Text.class, Text.class, IntWritable.class,
				conf);
		ChainMapper.addMapper(job, SecondMapper.class, Text.class, IntWritable.class, Text.class, Text.class, conf);
		ChainReducer.setReducer(job, MyReducer.class, Text.class, Text.class, Text.class, Text.class, conf);

		// 只设置最后Reducer输出的类型即可。
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		// 设置输出结果以key-value的map格式来输出
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
