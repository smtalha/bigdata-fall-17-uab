package com.reddithate;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class HpLevelByUserReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
	protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
		double totalComment = 0;
        double totalFreq = 0;
        for(DoubleWritable hateFreq : values) {
            totalFreq += hateFreq.get();
            totalComment++;
        }

        if (totalFreq == 0 || totalComment == 0) {
            return;
        }

        double hateFreqPerUser = totalFreq / totalComment;


        context.write(key, new DoubleWritable(hateFreqPerUser));
    }
}
