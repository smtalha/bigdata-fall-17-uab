package com.reddithate;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.google.gson.Gson;

public class HatefulRedditsMaper extends Mapper<Object, Text, Text, IntWritable> {
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String [] arr = value.toString().split("\t");
		
		Gson gson = new Gson();
		
		Map<String,Object> submission = gson.fromJson(arr[0].toString(), Map.class);
		
		context.write(new Text(submission.get("subreddit").toString()), new IntWritable(Integer.parseInt(arr[1])));
		
	}
}
