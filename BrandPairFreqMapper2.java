/*  
      @author Saurabh Nailwal      
*/

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class BrandPairFreqMapper2 extends MapReduceBase implements Mapper<LongWritable,Text,Text,IntWritable>{

	private final static IntWritable one = new IntWritable(1);
	private Text outKey = new Text();

	//map method that performs the tokenizer job and framing the initial key value pairs
	public void map(LongWritable key,Text value,OutputCollector<Text,IntWritable> output,Reporter reporter) throws IOException{

	System.out.println("first line:" +value.toString().trim());
	String[] brands = value.toString().trim().split("\\s");	
	int k=0;
	
	while (k < (brands.length-1)){        
		    if(brands[k+1]!= null)
		    { 		
		    	System.out.println("brand k"+ brands[k]);
		    	outKey.set("("+ brands[k] + "," + brands[k+1] + ")");		
			    System.out.println("outkey"+outKey);		
		
			    //sending to reducer through output collector 		
			    output.collect(outKey, one);
		    }
		    k++;
	}	
	
	
  }

} 

