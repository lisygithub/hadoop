package com.lisy.mr;

import com.lisy.comparator.CompositeKeyComparator;
import com.lisy.comparator.NaturalKeyGroupingComparator;
import com.lisy.partitioner.NaturalKeyPartitioner;
import com.lisy.writable.CompositeKey;
import com.lisy.writable.TimeSeriesData;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class SortByMRF_MovingAverageDriver extends Configured implements Tool{
    public static void main(String[] args) throws Exception {
         args = new String[3];
        args[0] = "E:\\workspace\\hadoop\\temp\\movingAverage";
        args[1] = "E:/workspace/hadoop/res/movingAverage";
        args[2] = "2";
        int run = ToolRunner.run(new SortByMRF_MovingAverageDriver(), args);
        System.exit(run);
    }
    @Override
    public int run(String[] args) throws Exception {

        Job job = Job.getInstance();
        job.setJobName("movingAverageJob");
        job.setJarByClass(SortByMRF_MovingAverageDriver.class);
        Path output = new Path(args[1]);

        FileSystem.get(getConf()).delete(output,true);

        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job, output);
        job.getConfiguration().setInt("window_size",Integer.parseInt(args[2]));

        job.setMapperClass(SortByMRF_MovingAverageMapper.class);
        job.setMapOutputKeyClass(CompositeKey.class);
        job.setMapOutputValueClass(TimeSeriesData.class);

        job.setSortComparatorClass(CompositeKeyComparator.class);
        job.setGroupingComparatorClass(NaturalKeyGroupingComparator.class);
        job.setPartitionerClass(NaturalKeyPartitioner.class);

        job.setReducerClass(SortByMRF_MovingAverageReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        boolean status = job.waitForCompletion(true);
        return status?1:0;
    }
}
