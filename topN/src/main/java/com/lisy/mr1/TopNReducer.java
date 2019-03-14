package com.lisy.mr1;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.SortedMap;
import java.util.TreeMap;

public class TopNReducer extends Reducer<NullWritable, Text,Text,Text> {
    private SortedMap<Double,String> topMap = new TreeMap<>();
    private int N;
    private String order;
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        this.N = context.getConfiguration().getInt("N",10);
        this.order = context.getConfiguration().get("ORDER","top");
    }

    @Override
    protected void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            String line = value.toString();
            String[] split = line.split("\t");
            topMap.put(Double.parseDouble(split[1]),line);
            if(topMap.size()>N){
                if("top".equals(order)){
                    topMap.remove(topMap.firstKey());
                }else{
                    topMap.remove(topMap.lastKey());
                }
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {

        for (String value : topMap.values()) {
            String[] split = value.split("\t");
            context.write(new Text(split[0]),new Text(split[1]));
        }
    }
}
