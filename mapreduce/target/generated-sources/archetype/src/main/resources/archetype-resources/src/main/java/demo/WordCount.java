#set( $symbol_pound = '#' )
#set( $symbol_dollar = '$' )
#set( $symbol_escape = '\' )
package ${package}.demo;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.${artifactId}.Job;
import org.apache.hadoop.${artifactId}.Mapper;
import org.apache.hadoop.${artifactId}.Reducer;
import org.apache.hadoop.${artifactId}.lib.input.FileInputFormat;
import org.apache.hadoop.${artifactId}.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordCount {
    public static class Map01 extends Mapper<LongWritable, Text,Text, IntWritable>{

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String valueString = value.toString();
            String[] split = valueString.split("${symbol_escape}t");
            for (String s : split) {
                context.write(new Text(s),new IntWritable(1));
            }
        }
    }

    public static class Reduce01 extends Reducer<Text,IntWritable,Text,IntWritable>{

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0 ;
            for (IntWritable value : values) {
                sum+=value.get();
            }
            context.write(key,new IntWritable(sum));
        }
    }

    public static void main(String[] args) {
        try {
            Job job = Job.getInstance();
            job.setJobName("mr_demo01");
            job.setJarByClass(WordCount.class);

            FileInputFormat.addInputPath(job,new Path("E:/temp/demo1.txt"));

            job.setMapperClass(Map01.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);

            //shuffer

            job.setReducerClass(Reduce01.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);

            FileOutputFormat.setOutputPath(job,new Path("E:/res"));
            job.submit();
            System.out.println(job.isSuccessful());
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

    }
}
