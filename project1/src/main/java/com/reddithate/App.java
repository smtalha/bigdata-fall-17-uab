package com.reddithate;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobPriority;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;


public class App {
		
	private static Logger logger = Logger.getLogger(App.class);
	
	private static enum JobType {
		RedditHateSpeechFilter,
		HatefulSubreddits,
		HpLevelByDate,
		HpLevelByUser,
		RaidUser,
		TemporalPattern,
		MostUsedHateWords
	}
	
	public static void main(String [] args) {
		try {
			int res = run(args, JobType.MostUsedHateWords);
			System.exit(res);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(255);
		}
	}
	
	static int run(String [] args, JobType jobType) throws IOException, InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "Reddit Hate Speech Job");
		
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
		
		switch (jobType) {
			case RedditHateSpeechFilter:
				job.setMapperClass(RedditMapper.class);
				job.setMapOutputKeyClass(Text.class);
				job.setMapOutputValueClass(IntWritable.class);
				job.setReducerClass(RedditReducer.class);
				job.setOutputKeyClass(Text.class);
				job.setOutputValueClass(IntWritable.class);
				break;
				
			case HatefulSubreddits:
				job.setMapperClass(HatefulRedditsMaper.class);
				job.setMapOutputKeyClass(Text.class);
				job.setMapOutputValueClass(IntWritable.class);
				job.setReducerClass(HatefulRedditsReducer.class);
				job.setOutputKeyClass(Text.class);
				job.setOutputValueClass(IntWritable.class);
				break;
				
			case HpLevelByDate:
				job.setMapperClass(HpLevelByDateMapper.class);
				job.setMapOutputKeyClass(Text.class);
				job.setMapOutputValueClass(DoubleWritable.class);
				job.setReducerClass(HpLevelByDateReducer.class);
				job.setOutputKeyClass(Text.class);
				job.setOutputValueClass(DoubleWritable.class);
				break;
				
			case HpLevelByUser:
				job.setMapperClass(HpLevelByUserMapper.class);
				job.setMapOutputKeyClass(Text.class);
				job.setMapOutputValueClass(DoubleWritable.class);
				job.setReducerClass(HpLevelByUserReducer.class);
				job.setOutputKeyClass(Text.class);
				job.setOutputValueClass(DoubleWritable.class);
				break;
				
			case RaidUser:
				job.setMapperClass(RaidUserMapper.class);
				job.setMapOutputKeyClass(Text.class);
				job.setMapOutputValueClass(DoubleWritable.class);
				job.setReducerClass(RaidUserReducer.class);
				job.setOutputKeyClass(Text.class);
				job.setOutputValueClass(DoubleWritable.class);
				break;
	
			case TemporalPattern:
				job.setMapperClass(TemporalMapper.class);
				job.setMapOutputKeyClass(LongWritable.class);
				job.setMapOutputValueClass(DoubleWritable.class);
				job.setReducerClass(TemporalReducer.class);
				job.setOutputKeyClass(LongWritable.class);
				job.setOutputValueClass(DoubleWritable.class);
				break;
				
			case MostUsedHateWords:
				job.setMapperClass(MostUserHateWordMapper.class);
				job.setMapOutputKeyClass(Text.class);
				job.setMapOutputValueClass(Text.class);
				job.setReducerClass(MostUsedHateWordReducer.class);
				job.setOutputKeyClass(Text.class);
				job.setOutputValueClass(Text.class);
				break;
				
			default:
				break;
		}
		
		logger.info("mapper class is " + job.getMapperClass());
		logger.info("mapper output key class is " + job.getMapOutputKeyClass());
		logger.info("mapper output value class is " + job.getMapOutputValueClass());
		logger.info("reducer class is " + job.getReducerClass());		
		logger.info("combiner class is " + job.getCombinerClass());		
		logger.info("output key class is " + job.getOutputKeyClass());
		logger.info("output value class is " + job.getOutputValueClass());

		
		job.setInputFormatClass(TextInputFormat.class);
		logger.info("input format class is " + job.getInputFormatClass());

		job.setOutputFormatClass(TextOutputFormat.class);
		logger.info("output format class is " + job.getOutputFormatClass());
		
		Path filePath = new Path(args[0]);
		logger.info("input path "+ filePath);
		FileInputFormat.setInputPaths(job, filePath);
		
		Path outputPath = new Path(args[1]);
		logger.info("output path "+ outputPath);
		FileOutputFormat.setOutputPath(job, outputPath);

		job.waitForCompletion(true);
		return 0;
	}
}
