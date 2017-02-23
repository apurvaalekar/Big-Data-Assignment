package assignment2.yelp;
import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
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
public class TopTen {

	
	public static class Top10Map extends Mapper<Object, Text, Text, IntWritable>{

     private Text word = new Text();
     private final static IntWritable one = new IntWritable(1);
     
     public void map(Object key, Text value, Context context ) throws IOException, InterruptedException {
   	  
   	  
   	 
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
   	 String addresss[]=address.split(" ");
   	 word.set(addresss[addresss.length-1]);
   	 context.write(word, one);
     }
 
 }
	 
	 public static class Combine extends Reducer<Text,IntWritable,Text,IntWritable> {
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
	 
	 public static class Reduce extends Reducer<Text,IntWritable,Text,IntWritable> {
		 private IntWritable result = new IntWritable();
		 private Text word = new Text();
         Map<String,Integer> map=new HashMap<String,Integer>();
         
		 public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
       int sum = 0;
       
       for (IntWritable val : values) {
           sum += val.get();
         }
         map.put(key.toString(), sum);  
		 }
		 
		 @Override
		protected void cleanup(Reducer<Text, IntWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			
			Map<String,Integer> sortedMap=sortByValue(map);
			int count=0;
			for(String s: sortedMap.keySet())
			{
				result.set(sortedMap.get(s));
				word.set(s);
				context.write(word, result);
				count++;
				if(count==10)
					break;
			}
			
		}
		 
		 private static Map<String, Integer> sortByValue(Map<String, Integer> unsortMap) {

		        List<Map.Entry<String, Integer>> list =
		                new LinkedList<Map.Entry<String, Integer>>(unsortMap.entrySet());

		        Collections.sort(list, new Comparator<Map.Entry<String, Integer>>() {
		            public int compare(Map.Entry<String, Integer> o1,
		                               Map.Entry<String, Integer> o2) {
		                return (o2.getValue()).compareTo(o1.getValue());
		            }
		        });

		        Map<String, Integer> sortedMap = new LinkedHashMap<String, Integer>();
		        for (Map.Entry<String, Integer> entry : list) {
		            sortedMap.put(entry.getKey(), entry.getValue());
		        }

		        return sortedMap;
		    }
  }
	 
	 public static void main(String[] args) throws Exception {
		    Configuration conf = new Configuration();
		    Job job = Job.getInstance(conf, "Top Ten Business");
		    job.setJarByClass(TopTen.class);
		    job.setMapperClass(Top10Map.class);
		    job.setCombinerClass(Combine.class);
		    job.setReducerClass(Reduce.class);
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(IntWritable.class);
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
   }
}
