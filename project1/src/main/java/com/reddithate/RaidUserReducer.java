package com.reddithate;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RaidUserReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
	protected void reduce(Text authorName, Iterable<DoubleWritable> hateFrequency, Context context) throws IOException, InterruptedException {
        double totalHateFq = 0;
        double totalDocument = 0;

        for(DoubleWritable hfc : hateFrequency) {
            totalHateFq += hfc.get();
            totalDocument++;
        }

        if(totalHateFq == 0 || totalDocument == 0) {
            return;
        }

        double hfcOfAuthor = totalHateFq / totalDocument;

        context.write(authorName, new DoubleWritable(hfcOfAuthor));
    }
}
