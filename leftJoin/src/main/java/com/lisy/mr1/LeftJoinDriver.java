package com.lisy.mr1;

import com.lisy.pair.PairOfStrings;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;


public class LeftJoinDriver implements Tool {
    public static void main(String[] args) throws Exception {
        args = new String[3];
        args[0] = "E:\\workspace\\hadoop\\temp\\users";
        args[1] = "E:\\workspace\\hadoop\\temp\\transactions";
        args[2] = "E:\\workspace\\hadoop\\temp\\output";
        int res = ToolRunner.run(new LeftJoinDriver(), args);
        System.exit(res);
    }
    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(), "leftJoin");
        job.setJarByClass(LeftJoinDriver.class);
        Path users = new Path(args[0]);
        Path transactions = new Path(args[1]);

        MultipleInputs.addInputPath(job,
                                    users,
                                    TextInputFormat.class,
                                    LeftJoinUserMapper.class
                                    );
        MultipleInputs.addInputPath(job,
                                    transactions,
                                    TextInputFormat.class,
                                    LeftJoinTransactionMapper.class
                                    );
        FileOutputFormat.setOutputPath(job,new Path(args[2]));
        job.setMapOutputKeyClass(PairOfStrings.class);
        job.setMapOutputValueClass(PairOfStrings.class);

        job.setPartitionerClass(SecondarySortPartitioner.class);
        job.setGroupingComparatorClass(SecondarySortGroupingComparator.class);

        job.setReducerClass(LeftJoinReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

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
