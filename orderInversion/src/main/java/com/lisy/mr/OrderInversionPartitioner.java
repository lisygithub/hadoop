package com.lisy.mr;

import com.lisy.pair.PairOfWords;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class OrderInversionPartitioner extends Partitioner<PairOfWords, IntWritable> {
    @Override
    public int getPartition(PairOfWords key, IntWritable value, int numPartitions) {
        return Math.abs(key.getWord().hashCode() % numPartitions);
    }
}
