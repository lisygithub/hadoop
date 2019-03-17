package com.lisy.demo;

import org.apache.commons.lang.time.DateUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.testArchetype.Job;
import org.apache.hadoop.testArchetype.Mapper;
import org.apache.hadoop.testArchetype.Partitioner;
import org.apache.hadoop.testArchetype.Reducer;
import org.apache.hadoop.testArchetype.lib.input.FileInputFormat;
import org.apache.hadoop.testArchetype.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.ParseException;

/**
 * 二次排序
 */
public class SecondarySortDriver2 {
    public static Logger logger = LoggerFactory.getLogger(SecondarySortDriver2.class);

    public static class CompositeKey implements WritableComparable<CompositeKey> {

        private String stockSymbol ;//股票代码
        private long timestamp ;//日期

        public CompositeKey(String stockSymbol, long timestamp) {
            set(stockSymbol,timestamp);
        }

        public CompositeKey() {
        }
        public void set(String stockSymbol,long timestamp) {
            this.stockSymbol = stockSymbol;
            this.timestamp = timestamp;
        }
        @Override
        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeUTF(stockSymbol);
            dataOutput.writeLong(timestamp);
        }
        @Override
        public void readFields(DataInput dataInput) throws IOException {
            stockSymbol = dataInput.readUTF();
            timestamp = dataInput.readLong();
        }

        @Override
        public int compareTo(CompositeKey o) {
            int compare = stockSymbol.compareTo(o.stockSymbol);
            if(0 == compare){
                compare = this.timestamp<o.getTimestamp()?-1:1;
            }
            return compare;
        }

        public String getStockSymbol() {
            return stockSymbol;
        }

        public void setStockSymbol(String stockSymbol) {
            this.stockSymbol = stockSymbol;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public void setTimestamp(long timestamp) {
            this.timestamp = timestamp;
        }
    }
    public static class CompositeKeyComparator extends WritableComparator{
        public CompositeKeyComparator() {
            super(CompositeKey.class,true);
        }

        @Override
        public int compare(WritableComparable wc1, WritableComparable wc2) {
            CompositeKey k1 = (CompositeKey) wc1;
            CompositeKey k2 = (CompositeKey) wc2;
            return k1.compareTo(k2);
        }
    }
    public static class NaturalValue implements Writable {
        private String yearMonthDay;
        private double money;

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeUTF(yearMonthDay);
            dataOutput.writeDouble(money);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            yearMonthDay = dataInput.readUTF();
            money = dataInput.readDouble();
        }

        public String getYearMonthDay() {
            return yearMonthDay;
        }

        public void setYearMonthDay(String yearMonthDay) {
            this.yearMonthDay = yearMonthDay;
        }

        public double getMoney() {
            return money;
        }

        public void setMoney(double money) {
            this.money = money;
        }
    }
    //定制分区器 年月分组
    public static class NaturalKeyPartitioner extends Partitioner<CompositeKey,NaturalValue>{
        @Override
        public int getPartition(CompositeKey compositeKey, NaturalValue naturalValue, int numPartitions) {
            return compositeKey.getStockSymbol().hashCode()%numPartitions;
        }
    }
    //分组比较器
    public static class NaturalKeyGroupingComparator extends WritableComparator{
        public NaturalKeyGroupingComparator() {
            super(CompositeKey.class,true);
        }

        @Override
        /**
         * 根据自然键指定分组到哪个reduce
         */
        public int compare(WritableComparable wc1, WritableComparable wc2) {
            CompositeKey k1 = (CompositeKey) wc1;
            CompositeKey k2 = (CompositeKey) wc2;
            int compareTo = k1.getStockSymbol().compareTo(k2.getStockSymbol());
            return compareTo;
        }
    }
    public static class SecondarySortMap extends Mapper<LongWritable, Text, CompositeKey, NaturalValue>{

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String valueString = value.toString();
            String[] split = valueString.split(",");
            String stockSymbol = split[0];
            String yearMonthDay = split[1];
            double money = Double.parseDouble(split[2]);

            try {
                CompositeKey compositeKey = new CompositeKey(stockSymbol,DateUtils.parseDate(yearMonthDay, new String[]{"yyyy-MM-dd"}).getTime());
                NaturalValue naturalValue = new NaturalValue();
                naturalValue.setYearMonthDay(yearMonthDay);
                naturalValue.setMoney(money);
                context.write(compositeKey,naturalValue);
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }
    }

    public static class SecondarySortReduce extends Reducer<CompositeKey,NaturalValue,Text,Text>{
        @Override
        protected void reduce(CompositeKey key, Iterable<NaturalValue> values, Context context) throws IOException, InterruptedException {
            StringBuilder sb = new StringBuilder();
            for (NaturalValue value : values) {
                sb.append("(");
                sb.append(value.getYearMonthDay());
                sb.append(",");
                sb.append(value.money);
                sb.append(")");
            }
            context.write(new Text(key.getStockSymbol()),new Text(sb.toString()));
        }
    }

    public static void main(String[] args) {
        try {
            Job job = Job.getInstance();
            job.setJobName("secondarySortJob2");
            job.setJarByClass(SecondarySortDriver2.class);

            FileInputFormat.addInputPath(job,new Path("E:\\workspace\\hadoop\\temp\\demo3.txt"));

            job.setMapperClass(SecondarySortMap.class);
            job.setMapOutputKeyClass(CompositeKey.class);
            job.setMapOutputValueClass(NaturalValue.class);
            job.setSortComparatorClass(CompositeKeyComparator.class);

            //shuffer
            job.setPartitionerClass(NaturalKeyPartitioner.class);
            job.setGroupingComparatorClass(NaturalKeyGroupingComparator.class);

            job.setReducerClass(SecondarySortReduce.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            FileOutputFormat.setOutputPath(job,new Path("E:\\workspace\\hadoop\\res\\res"+System.currentTimeMillis()));
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
