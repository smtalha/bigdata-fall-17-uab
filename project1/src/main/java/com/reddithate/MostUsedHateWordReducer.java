package com.reddithate;

import java.io.IOException;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MostUsedHateWordReducer extends Reducer<Text, Text, Text, Text> {
	
	private static HashMap<String, Long> hateWordsMap = new HashMap<String, Long>();
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		for (Text text : values) {
			countHateWords(text.toString());
		}
		
		if (hateWordsMap.isEmpty()) {
			return;
		}
		
		String maxKey = "";
		Long maxValue = new Long(0);
		
		String[] keys = new String[1];
		keys = hateWordsMap.keySet().toArray(keys);
		
		for (int i = 0; i < keys.length; i++) {
			Long val = hateWordsMap.get(keys[i]);
			if (val > maxValue) {
				maxValue = val;
				maxKey = keys[i];
			}
		}
		
		context.write(key, new Text(maxKey + "|" + maxValue));
	}
	
	private void countHateWords(String text) {				
		if (text == null) {
			return;
		}
		
		StringTokenizer tokenizer = new StringTokenizer(text, "|");
		
		while (tokenizer.hasMoreTokens()) {
			String word = tokenizer.nextToken().toLowerCase().trim();
			if (hateWordsMap.containsKey(word)) {
				hateWordsMap.put(word, hateWordsMap.get(word)+1);
			} else {
				hateWordsMap.put(word, new Long(1));
			}
		}
	}
}
