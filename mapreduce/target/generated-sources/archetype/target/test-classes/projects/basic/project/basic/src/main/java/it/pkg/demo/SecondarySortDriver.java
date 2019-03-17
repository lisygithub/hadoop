package it.pkg.demo;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.basic.Job;
import org.apache.hadoop.basic.Mapper;
import org.apache.hadoop.basic.Partitioner;
import org.apache.hadoop.basic.Reducer;
import org.apache.hadoop.basic.lib.input.FileInputFormat;
import org.apache.hadoop.basic.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Date;

/**
 * 二次排序
 */
public class SecondarySortDriver {
    public static Logger logger = LoggerFactory.getLogger(SecondarySortDriver.class);
    //二次排序键
    public static class DateTemperaturePair implements WritableComparable<DateTemperaturePair>{
        private Text yearMonth = new Text();//自然键
        private Text day = new Text();
        private IntWritable temperature = new IntWritable();//次键

        public DateTemperaturePair() {
        }

        public DateTemperaturePair(Text yearMonth, Text day, IntWritable temperature) {
            this.yearMonth = yearMonth;
            this.day = day;
            this.temperature = temperature;
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            yearMonth.write(dataOutput);
            day.write(dataOutput);
            temperature.write(dataOutput);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            yearMonth.readFields(dataInput);
            day.readFields(dataInput);
            temperature.readFields(dataInput);
        }

        @Override
        public int compareTo(DateTemperaturePair o) {
            int compareValue = this.getYearMonth().compareTo(o.getYearMonth());
            if(0 == compareValue){
                compareValue = this.getTemperature().compareTo(o.getTemperature());
            }
            //return -1*compareValue; //降序
            return compareValue;  //升序
        }

        public Text getYearMonth() {
            return yearMonth;
        }

        public void setYearMonth(String yearMonth) {
            this.yearMonth = new Text(yearMonth);
        }

        public Text getDay() {
            return day;
        }

        public void setDay(String day) {
            this.day = new Text(day);
        }

        public IntWritable getTemperature() {
            return temperature;
        }

        public void setTemperature(int temperature) {
            this.temperature = new IntWritable(temperature);
        }
    }
    //定制分区器 年月分组
    public static class DateTemperaturePartitioner extends Partitioner<DateTemperaturePair,Text>{
        @Override
        public int getPartition(DateTemperaturePair dateTemperaturePair, Text text, int numPartitions) {
            return Math.abs(dateTemperaturePair.getYearMonth().hashCode() % numPartitions);
        }
    }
    //分组比较器
    public static class DateTemperatureGroupingComparator extends WritableComparator{
        public DateTemperatureGroupingComparator() {
            super(DateTemperaturePair.class,true);
        }

        @Override
        /**
         * 这个比较器控制哪些键要
         * 分组到一个reduce() 方法调用
         */
        public int compare(WritableComparable wc1, WritableComparable wc2) {
            DateTemperaturePair p1 = (DateTemperaturePair) wc1;
            DateTemperaturePair p2 = (DateTemperaturePair) wc2;
            return p1.getYearMonth().compareTo(p2.getYearMonth());
        }
    }
    public static class Map2 extends Mapper<LongWritable, Text,DateTemperaturePair, IntWritable>{

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String valueString = value.toString();
            String[] split = valueString.split(",");
            String yearMonth = split[0]+"-"+split[1];
            String day = split[2];
            int temperature = Integer.parseInt(split[3]);
            DateTemperaturePair dateTemperaturePair = new DateTemperaturePair();
            dateTemperaturePair.setYearMonth(yearMonth);
            dateTemperaturePair.setDay(day);
            dateTemperaturePair.setTemperature(temperature);
            context.write(dateTemperaturePair,dateTemperaturePair.getTemperature());
        }
    }

    public static class Reduce2 extends Reducer<DateTemperaturePair,IntWritable,Text,Text>{
        @Override
        protected void reduce(DateTemperaturePair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            StringBuilder sb = new StringBuilder();
            for (IntWritable value : values) {
                sb.append(value);
                sb.append(",");
            }
            sb.deleteCharAt(sb.length()-1);
            context.write(key.getYearMonth(),new Text(sb.toString()));
        }
    }

    public static void main(String[] args) {
        try {
            Job job = Job.getInstance();
            job.setJobName("secondarySortJob");
            job.setJarByClass(SecondarySortDriver.class);

            FileInputFormat.addInputPath(job,new Path("E:\\workspace\\hadoop\\temp\\demo2.txt"));

            job.setMapperClass(Map2.class);
            job.setMapOutputKeyClass(DateTemperaturePair.class);
            job.setMapOutputValueClass(IntWritable.class);

            //shuffer
            job.setPartitionerClass(DateTemperaturePartitioner.class);
            job.setGroupingComparatorClass(DateTemperatureGroupingComparator.class);

            job.setReducerClass(Reduce2.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            FileOutputFormat.setOutputPath(job,new Path("E:/res"+System.currentTimeMillis()));

            boolean status = job.waitForCompletion(true);
            logger.info("result is " + status);
            System.exit(status?1:0);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

    }
}
