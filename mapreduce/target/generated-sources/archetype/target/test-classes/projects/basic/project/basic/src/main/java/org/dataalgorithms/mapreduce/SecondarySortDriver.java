package org.dataalgorithms.basic;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.basic.Job;
import org.apache.hadoop.basic.lib.input.FileInputFormat;
import org.apache.hadoop.basic.lib.input.TextInputFormat;
import org.apache.hadoop.basic.lib.output.FileOutputFormat;
import org.apache.hadoop.basic.lib.output.TextOutputFormat;

/** 
 * SecondarySortDriver is driver class for submitting secondary sort job to Hadoop.
 *
 * @author Mahmoud Parsian
 *
 */
public class SecondarySortDriver {

	public static void main(String[] args) throws Exception {

	    Job job = Job.getInstance();

	    job.setJarByClass(SecondarySortDriver.class);
        job.setJarByClass(SecondarySortMapper.class);
        job.setJarByClass(SecondarySortReducer.class);
	    
       // set mapper and reducer
	    job.setMapperClass(SecondarySortMapper.class);
	    job.setReducerClass(SecondarySortReducer.class);
	    
        // define mapper's output key-value
        job.setMapOutputKeyClass(CompositeKey.class);
        job.setMapOutputValueClass(NaturalValue.class);
              
        // define reducer's output key-value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // the following 3 setting are needed for "secondary sorting"
        // Partitioner decides which mapper output goes to which reducer 
        // based on mapper output key. In general, different key is in 
        // different group (Iterator at the reducer side). But sometimes, 
        // we want different key in the same group. This is the time for 
        // Output Value Grouping Comparator, which is used to group mapper 
        // output (similar to group by condition in SQL).  The Output Key 
        // Comparator is used during sort stage for the mapper output key.
	    job.setPartitionerClass(NaturalKeyPartitioner.class);
	    job.setGroupingComparatorClass(NaturalKeyGroupingComparator.class);
	    job.setSortComparatorClass(CompositeKeyComparator.class);
	    
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);

	    FileInputFormat.addInputPath(job, new Path("E:\\workspace\\hadoop\\temp\\demo3.txt"));
	    FileOutputFormat.setOutputPath(job, new Path("E:\\workspace\\hadoop\\res\\res"+System.currentTimeMillis()));

	    job.waitForCompletion(true);

	}
}
