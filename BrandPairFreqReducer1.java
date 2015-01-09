/*  
      @author Saurabh Nailwal      
*/

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class BrandPairFreqReducer1 extends MapReduceBase implements Reducer<Text,Text,Text,Text>{
	

	//reduce method accepts the Key Value pairs from mappers
	//do the aggregation based on keys and produce the final out put

	public void reduce(Text key, Iterator<Text> values,OutputCollector<Text,Text> output, Reporter reporter) throws IOException{

		//int sum = 0;
		
        String brandTs= "",finalValue="";
		
		/*iterates through all the values available with a key
         *and add them together
         *and give the final result as the key and sum of its values
         */

		while (values.hasNext()){
						
			brandTs = brandTs+ values.next().toString()+ " ";
            
		}		
		
		//Sorting Brands as per Timestamp
		String[] pairs = brandTs.toString().split("\\s"); 
		String[] delimSplit= null;
		int counter=0;  		
		
		//to change will need to be dynamic instead
		String[] brand = new String[1000];
		int[] ts = new int[1000];//
		
		
		for(int i=0;i < pairs.length;i++){
			
			StringTokenizer token = new StringTokenizer(pairs[i]);
					
			while (token.hasMoreTokens()){
			  
		      delimSplit = pairs[i].split("-");
		      
		      brand[counter]= delimSplit[0]; 
		      
		      ts[counter]= Integer.parseInt(delimSplit[1]);       
		      	      
		      token.nextToken();
		      
		      counter++;
		      
			}
			
		}
		
		// Bubble sort for sorting the timestamp and brand sequence
		for(int i=0;i<counter;i++)
		{
			for(int j=i+1;j<=counter;j++)
			{
			 	 
				if(ts[i] > ts[j])
				{
					int tempTs = ts[i];
					ts[i]=ts[j];
					ts[j]=tempTs;
					
					String tempB = brand[i];
					brand[i]=brand[j];
					brand[j]=tempB;
				}
			}
				
		}
		
		for(int i=0;i <= counter;i++){
			if(brand[i]!=null)
			{
		          finalValue = finalValue + brand[i] + " "; 
			}
		}
				
		output.collect(new Text(""),new Text(finalValue));

	}
} 
