/*  
      @author Saurabh Nailwal      
*/

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class BrandPairFreqMapper1 extends MapReduceBase implements Mapper<LongWritable,Text,Text,Text>{

	private Text userId = new Text();
	private Text brandTs= new Text();
	

	//map method that performs the tokenizer job and framing the initial key value pairs
	public void map(LongWritable key, Text value, OutputCollector<Text,Text> output, Reporter reporter) throws IOException{

	//taking one line at a time and tokenizing the same
	String line = value.toString();	
    System.out.println("line in mapper1"+ line);
	StringTokenizer tokenizer = new StringTokenizer(line);
	
    //iterating through all the words available in that line and forming the key value pair
	while (tokenizer.hasMoreTokens()){
             userId.set(tokenizer.nextToken());
             brandTs.set(tokenizer.nextToken()+"-"+tokenizer.nextToken().charAt(2)) ;
        
             System.out.println("Mapper1 key: "+ userId+ "- value : "+ brandTs);
             
             //sending to output collector which in turn passes the same to reducer
             output.collect(userId, brandTs);
      }
  }
} 