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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.google.gson.Gson;

public class MostUserHateWordMapper extends Mapper<Object, Text, Text, Text> {
	
	private static String hateDictionaryPath = "/user/smtalha/vocabulary.txt";
	private static String [] hateWords;
	
	
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		hateWords = readHateWordsFromFile(hateDictionaryPath, context);
		
		if (hateWords == null) {
			return;
		}
		
		String [] arr = value.toString().split("\t");
		
		Gson gson = new Gson();
		
		Map<String,Object> submission = gson.fromJson(arr[0].toString(), Map.class);
		
		if (submission.get("body") == null || submission.get("subreddit") == null) {
			return;
		}
		
		String result = "";
		
		String[] words = getHateWords(submission.get("body").toString());
		
		for (int i = 0; i < words.length; i++) {
			if (words[i] != null) {
				result = result.concat(words[i]);
				if (i != words.length-1) {
					result = result.concat("|");
				}
			}
		}
		
		context.write(new Text(submission.get("subreddit").toString()), new Text(result));
		
	}
	
	private String[] getHateWords(String text) {
		if (text == null) {
			return null;
		}
		
		ArrayList<String> words = new ArrayList<String>();
		String [] result = new String[1];
		
		StringTokenizer tokenizer = new StringTokenizer(text);
		
		while (tokenizer.hasMoreTokens()) {
			String word = tokenizer.nextToken().toLowerCase().trim();
			int index = Arrays.binarySearch(hateWords, word);
			if (index >= 0) {
				words.add(hateWords[index]);
			}
		}
		
		return words.toArray(result);
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
