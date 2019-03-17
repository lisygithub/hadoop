#set( $symbol_pound = '#' )
#set( $symbol_dollar = '$' )
#set( $symbol_escape = '\' )
package ${package}.mr;

import ${package}.pair.PairOfWords;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class RelativeFrequencyCombiner extends Reducer<PairOfWords, IntWritable,PairOfWords,IntWritable>
{
    @Override
    protected void reduce(PairOfWords key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int total = 0 ;
        for (IntWritable value : values) {
            total += value.get();
        }
        context.write(key,new IntWritable(total));
    }
}
