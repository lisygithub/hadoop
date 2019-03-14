package com.lisy.mr1;

import com.lisy.pair.PairOfStrings;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class SecondarySortGroupingComparator extends WritableComparator {
    public SecondarySortGroupingComparator() {
        super(PairOfStrings.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        PairOfStrings p1 = (PairOfStrings) a;
        PairOfStrings p2 = (PairOfStrings) b;
        return p1.getLeft().compareTo(p2.getLeft());
    }
}
