package com.lisy.mr1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.SortedMap;
import java.util.TreeMap;

public class TopNMapper extends Mapper<LongWritable, Text, NullWritable,Text> {
    private  SortedMap<Double,String> topMap = new TreeMap<Double,String>();
    private int N = 10;//默认top10
    private String order = "top";
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        this.N = context.getConfiguration().getInt("N",10);
        this.order = context.getConfiguration().get("ORDER","top");
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();
        String[] split = line.split("\t");
        topMap.put(Double.parseDouble(split[1]),line);
        if(topMap.size()>N){
            if("top".equals(order)){
                topMap.remove(topMap.firstKey());
            }else {
                topMap.remove(topMap.lastKey());
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (String value : topMap.values()) {
            context.write(NullWritable.get(),new Text(value));
        }
    }
}
