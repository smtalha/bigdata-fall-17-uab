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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.google.gson.Gson;

public class TemporalMapper extends Mapper<Object, Text, LongWritable, DoubleWritable> {

    protected void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

    	String [] arr = value.toString().split("\t");
		
		Gson gson = new Gson();
		
		Map<String,Object> submission = gson.fromJson(arr[0].toString(), Map.class);
		double hateTermCount = Double.parseDouble(arr[1]);
		
		if (submission.get("body") == null || submission.get("created_utc") == null) {
			return;
		}

        String bodyAsString = submission.get("body").toString();
        String createdTime = submission.get("created_utc").toString();

        long hourOfDay = calculateHours(createdTime);

        double total = bodyAsString.length() / 4;
		
		if (total == 0) {
			return;
		}

        double hateTermFrequency = hateTermCount / total;

        context.write(new LongWritable(hourOfDay), new DoubleWritable(hateTermFrequency));
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
}
