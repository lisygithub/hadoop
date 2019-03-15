package com.lisy.partitioner;

import com.lisy.writable.CompositeKey;
import com.lisy.writable.TimeSeriesData;
import org.apache.hadoop.mapreduce.Partitioner;

public class NaturalKeyPartitioner extends Partitioner<CompositeKey, TimeSeriesData> {
    @Override
    public int getPartition(CompositeKey compositeKey, TimeSeriesData timeSeriesData, int numPartitions) {
        return Math.abs(compositeKey.getStockSymbol().hashCode()%numPartitions);
    }
}
