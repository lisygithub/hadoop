package com.lisy.reduces;


import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class CommonFriendReducer extends Reducer<Text, Text, Text, Text>
{

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String friends = "";
        List<String> commonFriends = new ArrayList<>();
        for (Text value : values) {
            if("".equals(friends)){
                friends = value.toString();
            }else {
                String friends2 = value.toString();
                if(null == friends2 || "".equals(friends2)){
                    continue;
                }else {
                    String[] friendArray = friends2.split(",");
                    for (String friend : friendArray) {
                        boolean contains = friends.contains(friend);
                        if(contains){
                            commonFriends.add(friend);
                        }
                    }
                    friends = friends2;
                }
            }
        }
        Text outValue = new Text();
        outValue.set(commonFriends.toString());
        context.write(key,outValue);
    }
}
