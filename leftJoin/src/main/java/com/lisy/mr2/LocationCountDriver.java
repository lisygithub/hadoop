package com.lisy.mr2;

import com.lisy.mr1.LeftJoinDriver;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;

public class LocationCountDriver implements Tool {

    public static void main(String[] args) throws Exception {
        args = new String[2];
        args[0] = "E:\\workspace\\hadoop\\temp\\output";
        args[1] = "E:\\workspace\\hadoop\\res\\final";
        int run = ToolRunner.run(new LocationCountDriver(), args);
        System.exit(run);
    }
    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(), "locationCount");
        job.setJarByClass(LocationCountDriver.class);

        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        job.setMapperClass(LocationCountMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(LocationCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        boolean status = job.waitForCompletion(true);
        return status?1:0;
    }

    @Override
    public void setConf(Configuration conf) {

    }

    @Override
    public Configuration getConf() {
        return new Configuration();
    }
}
