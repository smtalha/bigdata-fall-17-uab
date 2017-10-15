package com.reddithate;

import com.google.gson.Gson;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

public class HpLevelByDateMapper extends Mapper<Object, Text, Text, DoubleWritable> {
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {		
		String [] arr = value.toString().split("\t");
		
		Gson gson = new Gson();
		
		Map<String,Object> submission = gson.fromJson(arr[0].toString(), Map.class);
		double hateWordCount = Double.parseDouble(arr[1]);
		
		if (submission.get("body") == null || submission.get("created_utc") == null) {
			return;
		}
		
		String bodyAsString = submission.get("body").toString();
		String createdTime = submission.get("created_utc").toString();		
		
		String date = calculateDate(createdTime);

//        StringTokenizer tokenizer = new StringTokenizer(bodyAsString);
//        double totalToken = 0;
//
//        while (tokenizer.hasMoreTokens()) {
//            totalToken++;
//        }
//
//        if(totalToken == 0) {
//            return;
//        }
		
		double total = bodyAsString.length() / 4;
		
		if (total == 0) {
			return;
		}

        double hateTermFrequency = hateWordCount / total;


        context.write(new Text(date), new DoubleWritable(hateTermFrequency));
	}
	
	private String calculateDate(String timestampString) {
        long timestamp = (Double.valueOf(timestampString).longValue() * 1000);
        Date date = new Date(timestamp);
        Calendar c = new GregorianCalendar(TimeZone.getTimeZone("GMT"));
        c.setTime(date);

        SimpleDateFormat dateFormat = new SimpleDateFormat("dd-M-yyyy");
        return dateFormat.format(c.getTime());
    }
}
