package assignment1b.part1.POS;

import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class Reduce extends Reducer<IntWritable,Text, IntWritable,Text> {


	public void reduce(IntWritable wordLength, Iterable<Text> instances, Context context)
			throws IOException, InterruptedException
	{
		int nounCount=0;
		int PluralCount=0;
		int nounPhrase=0;
		int verb=0;
		
		//System.out.println("in reducer " + wordLength + " ");
		
		/*int transitiveverb=0;
		int intransitiveverb=0;*/
		int adjective=0;
		int adverb=0;
		int conjunction=0;
		/*int preposition=0;
		int interjection=0;*/
		int pronoun=0;
		int palindrom=0;
	
		int count=0;

		// Sum up the instances of the current word.
		for (Text instance : instances) {
			
			//System.out.println("I am inside loop"+ instance );
			
			count++;
			
			System.out.println("Instance is" + instance);
			
		if(instance.toString().equals("NN") || instance.toString().equals("NNP") || instance.toString().equals("NNPS"))
			{
					nounCount++;
			}
			if(instance.toString().equals("NNS"))
				PluralCount++;
		
			
			if(instance.toString().equals("VB") || instance.toString().equals("VBD") || instance.toString().equals("VBG") || instance.toString().equals("VBN") )
				 verb++;
		
			if(instance.toString().equals("JJ") || instance.toString().equals("JJR") || instance.toString().equals("JJS"))
				adjective++;
		
			if(instance.toString().equals("RB") || instance.toString().equals("RBR") || instance.toString().equals("RBS"))
				adverb++;
		
			if(instance.toString().equals("PRP"))
				pronoun++;
		
			if(instance.toString().equals("Pal"))
				palindrom++;
		
			
		}
	//	System.out.println("Noun count:"+nounCount+" "+"Verb Count"+verb);
		
	// StringBuilder str = new StringBuilder();
	 String str = new String();
	 
	 
	 str = "Count of Words:"+ count +"  Distribution of POS: {" +" Noun: "+nounCount + ", Adjective:"+ adjective + " "+", Verb: "+verb+ " "+", Adverb: "+adverb + ", Pronoun : " + pronoun + "} Number of palindromes:" + palindrom;
	 
		/*str.append("Noun: "+nounCount + " 
		 * ");
		str.append("Verb: "+verb+ " ");*/
		//str.append("adjective: "+String.valueOf(adjective));
		//str.append("adverb: "+String.valueOf(adverb));
		
	
		
		//System.out.println("Str is   "+str);
	
		
		// Write the word and count to output.
		context.write(wordLength, new Text(str));
		
		
		}
}
