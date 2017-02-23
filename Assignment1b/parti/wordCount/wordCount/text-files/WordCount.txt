package assignment1b.part1.wordCount;

import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
// Wrappers for data types
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

// Configurable counters



public class WordCount extends Configured implements Tool {
	 Set<String> goodWords = new HashSet<String>();
	 Set<String> badWords = new HashSet<String>();
	
	
	public static void main(String[] args) {
		try {
			
			int res = ToolRunner.run(new WordCount(),args);
			System.exit(res);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public int run(String[] arg0) throws Exception {
		  
	    
		// TODO Auto-generated method stub
		Job job = Job.getInstance(getConf(), "wordcount");
		job.setJarByClass(this.getClass());
		FileInputFormat.addInputPath(job, new Path(arg0[0]));
		FileOutputFormat.setOutputPath(job, new Path(arg0[1]));
		
		
		
		
		job.setMapperClass(Map.class);
		//job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    int result = job.waitForCompletion(true) ? 0 : 1;
		
    
    Counters counters = job.getCounters();
    float good = counters.findCounter("assignment1b.part1.wordCount.Map$Gauge", "POSITIVE").getValue();
    float bad = counters.findCounter("assignment1b.part1.wordCount.Map$Gauge", "NEGATIVE").getValue();
    
    System.out.println("\n\n\n**********\n\n\n");
	System.out.println("Positive Words " + good );
	System.out.println("Negative Words " + bad );
    
    return result;
	}

}
