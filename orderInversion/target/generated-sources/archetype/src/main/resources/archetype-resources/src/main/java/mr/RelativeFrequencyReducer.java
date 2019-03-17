#set( $symbol_pound = '#' )
#set( $symbol_dollar = '$' )
#set( $symbol_escape = '\' )
package ${package}.mr;

import ${package}.pair.PairOfWords;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class RelativeFrequencyReducer extends Reducer<PairOfWords, IntWritable, Text, DoubleWritable>
{
    private double totalCount = 0;
    private String currentWord = "NOT_DEFINED";
    @Override
    protected void reduce(PairOfWords key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        if(key.getNeighbor().equals("*")){
            if(key.getWord().equals(currentWord)){
                totalCount += getTotalValues(values);
            }else {
                currentWord = key.getWord();
                totalCount += getTotalValues(values);
            }
        }else {
            double count = getTotalValues(values);
            double relativeCount = count /totalCount;
            context.write(new Text(key.toString()),new DoubleWritable(relativeCount));
        }
    }
    private double getTotalValues(Iterable<IntWritable> values){
        int total = 0;
        for (IntWritable value : values) {
            total+=value.get();
        }
        return total;
    }
}
