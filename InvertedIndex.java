package com.diptej.saner;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class InvertedIndex {

	public static class TokenizerMapper extends Mapper<LongWritable, Text, Text, Text> {
		private Text word = new Text();

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] contents = value.toString().split("\t");

			Text docID = new Text();
			docID.set(contents[0]);

			StringTokenizer itr = new StringTokenizer(contents[1].replaceAll("\\d"," ").replaceAll("[^a-zA-Z\\s]", "").toLowerCase());

			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				context.write(word, docID);
			}
		}
	}

	public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {
		private Text result;
		private Map<String, Integer> docIdCount;

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			docIdCount = new HashMap<>();
			result = new Text();

			for (Text val : values) {
				Integer count;
				String docId = val.toString();

				if ((count = docIdCount.putIfAbsent(docId, 1)) != null) {
					docIdCount.put(docId, ++count);
				}

			}

			StringBuilder sb = new StringBuilder();

			for (Map.Entry<String, Integer> entry : docIdCount.entrySet()) {
				sb.append(entry.getKey());
				sb.append(":");
				sb.append(entry.getValue());
				sb.append(" ");
			}

			result.set(sb.toString());

			context.write(key, new Text(sb.toString()));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "inverted index");

		job.setJarByClass(InvertedIndex.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setReducerClass(IntSumReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
