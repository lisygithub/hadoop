package com.lisy.mr1;

import com.lisy.pair.PairOfStrings;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class LeftJoinUserMapper extends Mapper<LongWritable, Text, PairOfStrings,PairOfStrings> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = StringUtils.split(value.toString(), ",");
        String userID = split[0];
        String locatoinID = split[1];
        context.write(new PairOfStrings(userID,"1"),new PairOfStrings("L",locatoinID));
    }
}
