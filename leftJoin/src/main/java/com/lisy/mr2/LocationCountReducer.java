package com.lisy.mr2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class LocationCountReducer extends Reducer<Text,Text,Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Set<Object> unique = new HashSet<>();
        for (Text value : values) {
            unique.add(value);
        }
        context.write(key,new IntWritable(unique.size()));
    }
}
