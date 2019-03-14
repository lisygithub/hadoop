package com.lisy.mr1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TopNDriver implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        String inputPath = args[0];
        String outputPath = args[1];
        String topN = args[2];
        if(args.length >= 4){
            String order = args[3];
            getConf().set("top.order",order);
        }

        Job job = Job.getInstance(getConf(), "topN");
        job.getConfiguration().setInt("N",Integer.parseInt(topN));
        job.setJarByClass(TopNDriver.class);

        FileInputFormat.addInputPath(job,new Path(inputPath));
        FileOutputFormat.setOutputPath(job,new Path(outputPath));

        job.setMapperClass(TopNMapper.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(TopNReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        boolean status = job.waitForCompletion(true);
        return status?1:0;
    }
    public static void main(String[] args) throws Exception {
        args= new String[3];
        args[0] = "E:/topNOut";//输入
        args[1] = "E:/final";//输出
        args[2] = "3";
        if(args.length<2){
            throw new IllegalArgumentException("Usage : com.lisy.mr1.AggregateByKeyDriver <inputPath> <outputPath>");
        }
        int status = ToolRunner.run(new TopNDriver(), args);
        System.exit(status);
    }

    @Override
    public void setConf(Configuration conf) {

    }

    @Override
    public Configuration getConf() {
        return new Configuration();
    }
}
