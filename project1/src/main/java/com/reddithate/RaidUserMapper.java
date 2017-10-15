package com.reddithate;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TimeZone;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.google.gson.Gson;

public class RaidUserMapper extends Mapper<Object, Text, Text, DoubleWritable> {
	
	private static final long HOUR_OF_DAY = 19;
	
	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		Gson gson = new Gson();
		
		Map<String,Object> submission = gson.fromJson(getJsonFromLine(value.toString()), Map.class);
		double hateTermCount = getHateCountFromLine(value.toString());
		
		if (submission.get("author") == null || submission.get("body") == null || submission.get("created_utc") == null) {
			return;
		}

        String authorName = submission.get("author").toString();
        String bodyAsString = submission.get("body").toString();
        String createdTime = submission.get("created_utc").toString();


        long hourOfDay = calculateHours(createdTime);

        if (hourOfDay != HOUR_OF_DAY) {
            return;
        }

        StringTokenizer tokenizer = new StringTokenizer(bodyAsString);
        double totalToken = 0;

        while (tokenizer.hasMoreTokens()) {
            totalToken++;
        }

        if (hateTermCount == 0 || totalToken == 0) {
            return;
        }

        double hateTermFrequency = hateTermCount / totalToken;

        context.write(new Text(authorName), new DoubleWritable(hateTermFrequency));
    }
	
	private long calculateHours(String timestampString) {
		long timestamp = (Double.valueOf(timestampString).longValue() * 1000);
        Date date = new Date(timestamp);
        Calendar c = new GregorianCalendar(TimeZone.getTimeZone("GMT"));
        c.setTime(date);

        SimpleDateFormat dateFormat = new SimpleDateFormat("HH");
        String hour = dateFormat.format(c.getTime());
        return Long.parseLong(hour);
    }
	
	private String getJsonFromLine(String line) {
		char [] lineArray = line.toCharArray();
		int jsonEndIndex = lineArray.length-1;
		
		for (int i = lineArray.length-1; i >= 0; i--) {
			if (lineArray[i] == '}') {
				jsonEndIndex = i;
				break;
			}
		}
		
		String jsonAsString = line.substring(0, jsonEndIndex+1);
		
		return jsonAsString;
	}
	
	private int getHateCountFromLine(String line) {
		char [] lineArray = line.toCharArray();
		int jsonEndIndex = lineArray.length-1;
		
		for (int i = lineArray.length-1; i >= 0; i--) {
			if (lineArray[i] == '}') {
				jsonEndIndex = i;
				break;
			}
		}
		
		int count = Integer.parseInt(line.substring(jsonEndIndex+1).trim());
		
		return count;
	}
}
