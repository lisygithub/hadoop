package com.lisy.driver;

import com.lisy.maps.CommonFriendMapper;
import com.lisy.reduces.CommonFriendReducer;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class CommonFriendDriver extends Configured implements Tool
{
    public static void main(String[] args) throws Exception {
        args = new String[3];
        args[0] = "E:\\workspace\\hadoop\\temp\\commonFriend";
        args[1] = "E:/workspace/hadoop/res/commonFriend";
        args[2] = "2";
        int run = ToolRunner.run(new CommonFriendDriver(), args);
        System.exit(run);
    }
    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance();
        job.setJobName("orderInversionJob");
        job.setJarByClass(CommonFriendDriver.class);

        Path input = new Path(args[0]);
        Path output = new Path(args[1]);
        /*int neighborWindow = Integer.parseInt(args[2]);
        job.getConfiguration().setInt("neighbor.window",neighborWindow);*/

        FileSystem.get(getConf()).delete(output,true);
        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);

        job.setMapperClass(CommonFriendMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(CommonFriendReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //job.setNumReduceTasks(3);

        boolean status = job.waitForCompletion(true);
        return status?1:0;
    }
}
