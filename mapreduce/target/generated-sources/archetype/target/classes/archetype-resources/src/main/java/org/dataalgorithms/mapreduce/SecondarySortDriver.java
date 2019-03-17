#set( $symbol_pound = '#' )
#set( $symbol_dollar = '$' )
#set( $symbol_escape = '\' )
package org.dataalgorithms.${artifactId};

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.${artifactId}.Job;
import org.apache.hadoop.${artifactId}.lib.input.FileInputFormat;
import org.apache.hadoop.${artifactId}.lib.input.TextInputFormat;
import org.apache.hadoop.${artifactId}.lib.output.FileOutputFormat;
import org.apache.hadoop.${artifactId}.lib.output.TextOutputFormat;

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

	    FileInputFormat.addInputPath(job, new Path("E:${symbol_escape}${symbol_escape}workspace${symbol_escape}${symbol_escape}hadoop${symbol_escape}${symbol_escape}temp${symbol_escape}${symbol_escape}demo3.txt"));
	    FileOutputFormat.setOutputPath(job, new Path("E:${symbol_escape}${symbol_escape}workspace${symbol_escape}${symbol_escape}hadoop${symbol_escape}${symbol_escape}res${symbol_escape}${symbol_escape}res"+System.currentTimeMillis()));

	    job.waitForCompletion(true);

	}
}
