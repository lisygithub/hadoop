package com.lisy.mr1;

import com.lisy.pair.PairOfStrings;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class LeftJoinReducer extends Reducer<PairOfStrings,PairOfStrings, Text,Text> {
    @Override
    protected void reduce(PairOfStrings key, Iterable<PairOfStrings> values, Context context) throws IOException, InterruptedException {
        String locationID = "undefined";
        for (PairOfStrings value : values) {
            if("L".equals(value.getLeft())){
                locationID = value.getRight();
                continue;
            }
            String productID = value.getRight();
            context.write(new Text(productID),new Text(locationID));
        }
    }
}
