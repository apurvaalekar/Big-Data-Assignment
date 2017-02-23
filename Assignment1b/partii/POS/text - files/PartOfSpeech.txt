package assignment1b.part1.POS;


import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



import org.apache.hadoop.mapreduce.Job;

public class PartOfSpeech extends Configured implements Tool{
	
	public static void main(String[] args) {
		try {
			int res = ToolRunner.run(new PartOfSpeech(),args);
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
		job.setNumReduceTasks(1);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(Text.class);
    int result = job.waitForCompletion(true) ? 0 : 1;
		return result;
	}
	

}
