package com.lisy.mr;

import com.lisy.util.MovingAverage;
import com.lisy.writable.CompositeKey;
import com.lisy.writable.TimeSeriesData;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;

import java.io.IOException;

public class SortByMRF_MovingAverageReducer extends Reducer<CompositeKey, TimeSeriesData, Text, Text> {
    private int WS = 2;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        WS = context.getConfiguration().getInt("window_size", 2);
    }

    @Override
    protected void reduce(CompositeKey key, Iterable<TimeSeriesData> values, Context context) throws IOException, InterruptedException {
        Text outKey = new Text();
        Text outValue = new Text();
        MovingAverage movingAverage = new MovingAverage(WS);
        for (TimeSeriesData value : values) {
            movingAverage.addNewNumber(value.getPrice());
            double average = movingAverage.getMovingAverage();
            outKey.set(key.getStockSymbol());
            outValue.set(value.getTimeFormat()+","+average);
            context.write(outKey,outValue);
        }
    }
}
