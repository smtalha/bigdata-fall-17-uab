package com.reddithate;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class TemporalReducer extends Reducer<Object, DoubleWritable, LongWritable, DoubleWritable> {
	protected void reduce(Object key, Iterable<DoubleWritable> freqValues, Context context) throws IOException, InterruptedException {
        double totalFrequency = 0;
        double totalDocument = 0;

        for(DoubleWritable freqValue : freqValues) {
            totalFrequency += freqValue.get();
            totalDocument++;
        }

        if (totalFrequency == 0 || totalDocument == 0) {
            return;
        }

        double hateWordsFreq = totalFrequency / totalDocument;

        context.write((LongWritable)key, new DoubleWritable(hateWordsFreq));
    }
}
