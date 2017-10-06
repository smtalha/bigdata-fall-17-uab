package com.reddithate;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Mapper;

import com.google.gson.Gson;

public class RedditMapper extends Mapper<Object, Text, Text, IntWritable> {
	
	private static String hateDictionaryPath = "/user/smtalha/vocabulary.txt";
	private static String [] hateWords;
	
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		hateWords = readHateWordsFromFile(hateDictionaryPath, context);
		
		Gson gson = new Gson();
		Map<String,Object> submission = gson.fromJson(value.toString(), Map.class);

		if (submission.get("body") != null) {
			//comment
			int hateWordsCount = getHateWordsCount(submission.get("body").toString());			
			context.write(new Text(gson.toJson(submission)), new IntWritable(hateWordsCount));
			return;
		}
		
		if (submission.get("title") != null || submission.get("selftext") != null) {
			//submission
			int hateWordsCount = getHateWordsCount(submission.get("title").toString());
			int hateWordsCount2 = getHateWordsCount(submission.get("selftext").toString());
			context.write(new Text(gson.toJson(submission)), new IntWritable(hateWordsCount + hateWordsCount2));
			return;
		}
	}

	private int getHateWordsCount(String line) {
		int hateWordsCount = 0;
		
		if (line == null) {
			return hateWordsCount;
		}
		
		StringTokenizer tokenizer = new StringTokenizer(line);
		
		while (tokenizer.hasMoreTokens()) {
			String word = tokenizer.nextToken().toLowerCase().trim();
			int index = Arrays.binarySearch(hateWords, word);
			if (index >= 0) {
				hateWordsCount++;
			}
		}
		return hateWordsCount;
	}
	
	private static String[] readHateWordsFromFile(String path, Context context) {
		ArrayList<String> words = new ArrayList<String>();
		
		if (hateWords != null && hateWords.length > 0) {
			return hateWords;
		}
	    
	    try {
	    	Path pt=new Path(path);
		    FileSystem fs = FileSystem.get(context.getConfiguration());
		    BufferedReader br =new BufferedReader(new InputStreamReader(fs.open(pt)));
		    
		    String line = "";
		    
		    while ((line = br.readLine()) != null) {
	    		String[] vocab  = line.split(";");
	    		words.add(vocab[0].toLowerCase().trim());
	    	}
	    	String [] arr = new String[words.size()];
		    br.close();
		    return words.toArray(arr);
	    } 
	    catch (Exception ex) {
	    	return null;
	    }
	}
}
