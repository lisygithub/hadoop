package com.lisy.maps;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CommonFriendMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = StringUtils.split(value.toString(), ',');
        if(null == split || split.length == 0){
            return;
        }
        Text outKey = new Text();
        Text outValue = new Text();
        StringBuilder outputBuilder = new StringBuilder();
        String me = split[0];
        List<String> outKeyList = new ArrayList<>();
        for (String fr : split) {
            if (me.equals(fr)) {
                continue;
            }
            outputBuilder.append(fr).append(",");
            String sortAndConcat = getSortAndConcat(me, fr);
            outKeyList.add(sortAndConcat);

        }
        String outputString = outputBuilder.deleteCharAt(outputBuilder.length() - 1).toString();
        outValue.set(outputString);
        for (String outkeyStirng : outKeyList) {
            outKey.set(outkeyStirng);
            context.write(outKey,outValue);
        }

    }
    private String getSortAndConcat(String value1,String value2){
        if(value1.compareTo(value2)<0){
            return value1+","+value2;
        }else {
            return value2+","+value1;
        }
    }
}
