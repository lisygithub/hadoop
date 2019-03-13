import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.SortedMap;
import java.util.TreeMap;

public class TopNReducer extends Reducer<NullWritable, Text,Text,Text> {
    private SortedMap<Double,String> topMap = new TreeMap<>();
    private int N;
    private String order;
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        N = conf.getInt("top.n",10);
        order = conf.get("top.order");
        super.setup(context);
    }

    @Override
    protected void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            String line = value.toString();
            String[] split = line.split(",");
            topMap.put(Double.parseDouble(split[1]),line);
            if(topMap.size()>N){
                if("top".equals(order)){
                    topMap.remove(topMap.firstKey());
                }else if("bottom".equals(order)){
                    topMap.remove(topMap.lastKey());
                }
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {

        for (String value : topMap.values()) {
            String[] split = value.split(",");
            context.write(new Text(split[0]),new Text(split[1]));
        }
    }
}
