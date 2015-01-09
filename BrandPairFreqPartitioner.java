/*  
      @author Saurabh Nailwal      
*/

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class BrandPairFreqPartitioner extends MapReduceBase implements Partitioner<Text,Text> {

		
	public int getPartition(Text key, Text value, int numReduceTasks){

		int userId = Integer.parseInt(key.toString());	

		//this is done to avoid performing mod with 0
		if(numReduceTasks == 0)
			return 0;

		//Id is greater than 1500, assign partition 0
		if(userId < 1200){
			return 0;
		}
		
		if(userId >=1200 && userId <1500)
		{
			//Id is less than 1500, assign partition 1
			return 1 % numReduceTasks;
		}
		else
		{
			return 2 % numReduceTasks;
		}
	}
		
} 

