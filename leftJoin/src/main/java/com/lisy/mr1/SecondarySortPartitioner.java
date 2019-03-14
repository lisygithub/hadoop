package com.lisy.mr1;

import com.lisy.pair.PairOfStrings;
import org.apache.hadoop.mapreduce.Partitioner;

public class SecondarySortPartitioner extends Partitioner<PairOfStrings,PairOfStrings> {
    @Override
    public int getPartition(PairOfStrings pair, PairOfStrings pair2, int numPartitions) {
        return (pair.getLeft().hashCode()&Integer.MAX_VALUE)%numPartitions;
    }
}
