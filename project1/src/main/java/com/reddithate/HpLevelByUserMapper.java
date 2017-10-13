package com.reddithate;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import com.google.gson.Gson;

import java.io.IOException;
import java.util.Map;
import java.util.StringTokenizer;

public class HpLevelByUserMapper extends Mapper<Object, Text, Text, DoubleWritable> {
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String [] arr = value.toString().split("\t");
		
		Gson gson = new Gson();
		
		Map<String,Object> submission = gson.fromJson(arr[0].toString(), Map.class);
		double hateWordCount = Double.parseDouble(arr[1]);
		
		if (submission.get("body") == null || submission.get("author") == null) {
			return;
		}
		
		String bodyAsString = submission.get("body").toString();
		String author = submission.get("author").toString();
		
		
        StringTokenizer tokenizer = new StringTokenizer(bodyAsString);
        double totalToken = 0;

        while (tokenizer.hasMoreTokens()) {
            totalToken++;
        }

        if(totalToken == 0) {
            return;
        }

        double hateTermFrequency = hateWordCount / totalToken;


        context.write(new Text(author), new DoubleWritable(hateTermFrequency));
	}
}
