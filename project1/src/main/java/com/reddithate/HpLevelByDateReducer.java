package com.reddithate;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class HpLevelByDateReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
	protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        double totalHateFrequency = 0;
        double totalDocument = 0;

        for(DoubleWritable value : values) {
            totalHateFrequency += value.get();
            totalDocument++;
        }

        if (totalHateFrequency == 0 || totalDocument == 0) {
            return;
        }

        double hateFrequency = totalHateFrequency / totalDocument;

        context.write(key, new DoubleWritable(hateFrequency));
    }
}
