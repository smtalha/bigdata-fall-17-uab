package com.reddithate;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;


public class App {
		
	private static Logger logger = Logger.getLogger(App.class);
	
	public static void main(String [] args) {
		try {
			int res = run(args);
			System.exit(res);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(255);
		}
	}
	
	static int run(String [] args) throws IOException, InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
	    //Job job = Job.getInstance(conf, "Reddit Hate Speech Job");
	    Job job = Job.getInstance(conf, "Most Hateful Reddits Job");
		
	    job.setJarByClass(App.class);
		
		logger.info("job " + job.getJobName() + " [" + job.getJar()
				+ "] started with the following arguments: "
				+ Arrays.toString(args));

		if (args.length < 2) {
			logger.warn("to run this jar are necessary at 2 parameters \""
					+ job.getJar()
					+ " input_files output_directory");
			return 1;
		}
		
		//job.setMapperClass(RedditMapper.class);
		job.setMapperClass(HatefulRedditsMaper.class);
		logger.info("mapper class is " + job.getMapperClass());
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		logger.info("mapper output key class is " + job.getMapOutputKeyClass());
		logger.info("mapper output value class is " + job.getMapOutputValueClass());

		//job.setReducerClass(RedditReducer.class);
		job.setReducerClass(HatefulRedditsReducer.class);
		logger.info("reducer class is " + job.getReducerClass());
		//job.setCombinerClass(RedditReducer.class);
		job.setCombinerClass(HatefulRedditsReducer.class);
		logger.info("combiner class is " + job.getCombinerClass());

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		logger.info("output key class is " + job.getOutputKeyClass());
		logger.info("output value class is " + job.getOutputValueClass());

		job.setInputFormatClass(TextInputFormat.class);
		logger.info("input format class is " + job.getInputFormatClass());

		job.setOutputFormatClass(TextOutputFormat.class);
		logger.info("output format class is " + job.getOutputFormatClass());

//		Path filePath = new Path(args[0]);
//		logger.info("input path "+ filePath);
//		FileInputFormat.setInputPaths(job, filePath);
//
//		Path outputPath = new Path(args[1]);
//		logger.info("output path "+ outputPath);
//		FileOutputFormat.setOutputPath(job, outputPath);
		
		Path filePath = new Path(args[0]);
		Path filePath2 = new Path(args[1]);
		logger.info("input path "+ filePath);
		logger.info("input path "+ filePath2);
		FileInputFormat.setInputPaths(job, filePath);
		FileInputFormat.setInputPaths(job, filePath2);

		Path outputPath = new Path(args[2]);
		logger.info("output path "+ outputPath);
		FileOutputFormat.setOutputPath(job, outputPath);

		job.waitForCompletion(true);
		return 0;
	}
}
