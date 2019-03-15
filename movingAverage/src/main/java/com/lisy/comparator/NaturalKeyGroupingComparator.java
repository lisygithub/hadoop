package com.lisy.comparator;

import com.lisy.writable.CompositeKey;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class NaturalKeyGroupingComparator extends WritableComparator {
    protected NaturalKeyGroupingComparator() {
        super(CompositeKey.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        CompositeKey k1 = (CompositeKey) a;
        CompositeKey k2 = (CompositeKey) b;
        return k1.getStockSymbol().compareTo(k2.getStockSymbol());
    }
}
