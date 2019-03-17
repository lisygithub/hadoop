#set( $symbol_pound = '#' )
#set( $symbol_dollar = '$' )
#set( $symbol_escape = '\' )
package ${package}.mr;

import ${package}.pair.PairOfWords;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class OrderInversionPartitioner extends Partitioner<PairOfWords, IntWritable> {
    @Override
    public int getPartition(PairOfWords key, IntWritable value, int numPartitions) {
        return Math.abs(key.getWord().hashCode() % numPartitions);
    }
}
