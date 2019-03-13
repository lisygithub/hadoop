import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import sun.plugin.dom.core.Text;

public class TopNDriver implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        String inputPath = args[0];
        String outputPath = args[1];
        String topN = args[2];
        String order = args[3];

        Job job = Job.getInstance(getConf(), "topN");
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
        if(args.length<2){
            throw new IllegalArgumentException("Usage : AggregateByKeyDriver <inputPath> <outputPath>");
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
