package com.lisy.mr;

import com.lisy.pair.PairOfWords;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class RelativeFrequencyDriver extends Configured implements Tool
{
    public static void main(String[] args) throws Exception {
       /* args = new String[3];
        args[0] = "E:\\workspace\\hadoop\\temp\\orderInversion";
        args[1] = "E:/workspace/hadoop/res/orderInversion";
        args[2] = "2";*/
        int run = ToolRunner.run(new RelativeFrequencyDriver(), args);
        System.exit(run);
    }
    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance();
        job.setJobName("orderInversionJob");
        job.setJarByClass(RelativeFrequencyDriver.class);

        Path input = new Path(args[0]);
        Path output = new Path(args[1]);
        int neighborWindow = Integer.parseInt(args[2]);
        job.getConfiguration().setInt("neighbor.window",neighborWindow);

        FileSystem.get(getConf()).delete(output,true);
        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);

        job.setMapperClass(RelativeFrequencyMapper.class);
        job.setMapOutputKeyClass(PairOfWords.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setPartitionerClass(OrderInversionPartitioner.class);

        job.setCombinerClass(RelativeFrequencyCombiner.class);
        job.setReducerClass(RelativeFrequencyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setNumReduceTasks(3);

        boolean status = job.waitForCompletion(true);
        return status?1:0;
    }
}
