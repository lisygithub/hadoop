package com.lisy.mr;

import com.lisy.pair.PairOfWords;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class RelativeFrequencyMapper extends Mapper<LongWritable, Text, PairOfWords, IntWritable> {

    private int neighborWindow = 2;
    private PairOfWords pair = new PairOfWords();
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        neighborWindow = context.getConfiguration().getInt("neighbor.window",2);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] words = StringUtils.split(value.toString(), " ");
        if(words.length<2){
            return;
        }
        for (int i = 0; i < words.length; i++) {
            String word = words[i];
            pair.setWord(word);
            int start = i - neighborWindow < 0 ? 0 : i-neighborWindow;
            int end = i + neighborWindow >= words.length ? words.length-1:i+neighborWindow;
            for (int j = start; j <= end; j++) {
                if(i == j){
                    continue;
                }
                String neighbor = words[j];
                pair.setNeighbor(neighbor);
                context.write(pair,new IntWritable(1));
            }
            pair.setNeighbor("*");
            context.write(pair,new IntWritable(end-start));
        }

    }
}
