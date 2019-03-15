package com.lisy.mr;

import com.lisy.util.DateUtil;
import com.lisy.writable.CompositeKey;
import com.lisy.writable.TimeSeriesData;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;

import java.io.IOException;

public class SortByMRF_MovingAverageMapper extends Mapper<LongWritable, Text, CompositeKey, TimeSeriesData> {

    private CompositeKey compositeKey = new CompositeKey();
    private TimeSeriesData timeSeriesData = new TimeSeriesData();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = StringUtils.split(value.toString(), ',');
        String stockSymbol = split[0];
        String timeFormat = split[1];// yyyy-MM-dd
        String priceString = split[2];//323.32
        long timestamp = DateUtil.getDate(timeFormat).getTime();
        compositeKey.set(stockSymbol,timestamp);
        timeSeriesData.set(timeFormat,Double.parseDouble(priceString));
        context.write(compositeKey,timeSeriesData);
    }
}
