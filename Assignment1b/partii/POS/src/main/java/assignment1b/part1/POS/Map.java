package assignment1b.part1.POS;
import edu.stanford.nlp.tagger.maxent.MaxentTagger;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
// Regular expression utility
import java.util.regex.Pattern;

// Wrappers for values
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
public class Map  extends Mapper<LongWritable, Text,IntWritable,Text> {

			private static final 
					Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");
			private HashMap<String,String> database = new HashMap<String,String>();
	
	public static boolean isPalindrome(String str) {
	    return str.equals(new StringBuilder(str).reverse().toString());
	}
	public void map(LongWritable offset, Text lineText, Context context)
		        throws IOException, InterruptedException {
			
		
		try {
			MaxentTagger tagger = new MaxentTagger("bidirectional-distsim-wsj-0-18.tagger");
		
		
				String line = lineText.toString();
				
				line = line.toLowerCase();

				// Store each the current word in the queue for processing.
				
				
				for (String word : WORD_BOUNDARY.split(line))
				{
					if (word.isEmpty() ) 
						continue;
					if(word.length()>=5)
					{
						//System.out.println("In MAPPER" + word.length());
						
						String tagged = tagger.tagString(word);
						
						String[] tag = tagged.split("/");
						
						System.out.println("#####################################");
						System.out.println("Tagged POS is :" + tag[1]);
						System.out.println("#####################################");
						
						if(database.containsKey(word))
							context.write(new IntWritable(word.length()),new Text(tag[1].trim()));
						
					
					if(isPalindrome(word))
					context.write(new IntWritable(word.length()),new Text("Pal"));
					}
					
				}
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}	
		
	}
}
