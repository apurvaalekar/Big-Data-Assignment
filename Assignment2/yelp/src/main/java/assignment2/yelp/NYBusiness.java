package assignment2.yelp;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class NYBusiness {
	public static class Map extends Mapper<Object, Text, Text, Text>{

     private Text buisness_id = new Text();
     private Text add = new Text();
  
     
     public void map(Object key, Text value, Context context
             ) throws IOException, InterruptedException {
   	  

    	 StringTokenizer tokenizer=new StringTokenizer(value.toString(), "^");
   	  int count=0;
   	  String address=null;
   	  String bid=null;
   	  String wordl;
   	  while(tokenizer.hasMoreTokens())
   	  {
   		  count++;
   		  wordl=tokenizer.nextToken().toString();
   		  if(count==1)
   			bid=wordl;
   		  if(count==2)
   		  {
   			  address=wordl;
   			  break;
   		  }
   		  
   	  }
    	 
   	
   	  if(address!=null && address.contains("NY"))
   	  {
   		  add.set(address);
   		buisness_id.set(bid);
   		  context.write(buisness_id, new Text(add));
   	  }
     }
 
 }
	 
	 public static class Reduce extends Reducer<Text,Text,Text,Text> {
		 
		public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException 
		{
		 for(Text words: values)
            context.write(key, words);     
		 }
  }
	 
	 public static void main(String[] args) throws Exception {
		    Configuration conf = new Configuration();
		    Job job = Job.getInstance(conf, "NY business");
		    job.setJarByClass(NYBusiness.class);
		    job.setMapperClass(Map.class);
		    //job.setCombinerClass(Reduce.class);
		    job.setReducerClass(Reduce.class);
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(Text.class);
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
   }

}
