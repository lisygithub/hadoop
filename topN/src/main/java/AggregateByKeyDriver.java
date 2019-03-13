import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import javax.xml.soap.Text;

public class AggregateByKeyDriver implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(),"aggregate_job" );
        job.setJarByClass(AggregateByKeyDriver.class);

        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        job.setMapperClass(AggregateByKeyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(AggregateByKeyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        boolean isSuccess = job.waitForCompletion(true);

        return isSuccess?1:0;
    }

    @Override
    public void setConf(Configuration conf) {

    }

    @Override
    public Configuration getConf() {
        Configuration entries = new Configuration();
        return entries;
    }

    public static void main(String[] args) throws Exception {
        if(args.length<2){
            throw new IllegalArgumentException("Usage : AggregateByKeyDriver <inputPath> <outputPath>");
        }
        int status = ToolRunner.run(new AggregateByKeyDriver(), args);
        System.exit(status);
    }
}
