package assignment1b.part1.wordCount;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
// Regular expression utility
import java.util.regex.Pattern;

// Wrappers for values
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
public class Map  extends Mapper<LongWritable, Text, Text, IntWritable> {
	enum Gauge{POSITIVE, NEGATIVE}
	
	// IntWritable object set to the value 1 as counting increment.
	private final static IntWritable one = new IntWritable(1);
	// HashSets for filter terms.
			
			private Set<String> goodWords = new HashSet<String>();
			private Set<String> badWords = new HashSet<String>();
			
			// Word boundary defined as whitespace-characters-word boundary-whitespace 
			private static final 
					Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");
	private Set<String> parsePositive(String positiveFilename) {
		try {
			BufferedReader fis = new BufferedReader(new FileReader(new File(positiveFilename)));
			String goodWord;
			int counter=1;
			while ((goodWord = fis.readLine()) != null) {
				if(counter>35)
					goodWords.add(goodWord);
				counter++;
			}
			fis.close();
		} catch (IOException ioe) {
			System.err.println("Caught exception parsing positive file" );
			ioe.printStackTrace();
		}
		return goodWords;
	}
	private Set<String> parseNegative(String negativeFilename) {
		try {
			BufferedReader fis = new BufferedReader(new FileReader(new File(negativeFilename)));
			String badword;
			int counter=1;
			while ((badword = fis.readLine()) != null) {
				if(counter>35)
					badWords.add(badword);
				counter++;
			}
			
			fis.close();
		} catch (IOException ioe) {
			System.err.println("Caught exception parsing positive file" );
			ioe.printStackTrace();
		}
		return badWords;
	}
	
		
		
		public void map(LongWritable offset, Text lineText, Context context)
		        throws IOException, InterruptedException {
			
			goodWords = parsePositive("positive-words.txt");
			badWords = parseNegative("negative-words.txt");
							
				String line = lineText.toString();
				
				line = line.toLowerCase();

				// Store each the current word in the queue for processing.
				
				Text Positive = new Text("POSITIVE");
				Text negative = new Text("NEGATIVE");
				for (String word : WORD_BOUNDARY.split(line))
				{
					if (word.isEmpty() ) 
						continue;
					// Count instances of each (non-skipped) word.
			
					//context.write(currentWord,one);         

					// Filter and count "good" words.
					if (goodWords.contains(word)) {
						
						context.write(Positive,one);
							context.getCounter(Gauge.POSITIVE).increment(1);
							
					}

					// Filter and count "bad" words.
					if (badWords.contains(word)) {
						context.write(negative,one);
						context.getCounter(Gauge.NEGATIVE).increment(1);
							
					}
				}
			}
}
