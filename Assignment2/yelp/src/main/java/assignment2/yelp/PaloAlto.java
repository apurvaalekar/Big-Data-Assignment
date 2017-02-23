package assignment2.yelp;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class PaloAlto {
	public static class Map extends Mapper<Object, Text, Text, IntWritable>{

     private Text word = new Text();
     private final static IntWritable one = new IntWritable(1);
     
     public void map(Object key, Text value, Context context
             ) throws IOException, InterruptedException {
   	  
    	 StringTokenizer tokenizer=new StringTokenizer(value.toString(), "^");
   	  int count=0;
   	  String address=null;
   	  String wordl;
   	  while(tokenizer.hasMoreTokens())
   	  {
   		  count++;
   		  wordl=tokenizer.nextToken().toString();
   		  if(count==2)
   		  {
   			  address=wordl;
   			  break;
   		  }
   		  
   	  }
   	  
   	  if(address!=null && address.contains("Palo Alto"))
   	  {
   		  word.set(address);
   		  context.write(word, one);
   	  }
     }
 
 }
	 
	 public static class Reduce
    extends Reducer<Text,IntWritable,Text,IntWritable> {
		 private IntWritable result = new IntWritable();
		 

		 public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
       int sum = 0;
       
       for (IntWritable val : values) {
           sum += val.get();
         }
         result.set(sum);
         context.write(key, result);     
		 }
  }
	 
	 public static void main(String[] args) throws Exception {
		    Configuration conf = new Configuration();
		    Job job = Job.getInstance(conf, "Palo Alto");
		    job.setJarByClass(PaloAlto.class);
		    job.setMapperClass(Map.class);
		    job.setCombinerClass(Reduce.class);
		    job.setReducerClass(Reduce.class);
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(IntWritable.class);
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
   }

}
