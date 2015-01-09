/*  
      @author Saurabh Nailwal      
*/

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;


public class BrandPairFreqDriver extends Configured implements Tool{

	public int run(String[] args) throws Exception{

		//creating a JobConf object and assigning a job name for identification purposes
		JobConf job1 = new JobConf(getConf(),BrandPairFreqDriver.class);
        job1.setJobName("asg1_Job1");    
        

        //Setting configuration object with the Data Type of output Key and Value
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        //Providing the mapper and reducer class names
        job1.setJarByClass(BrandPairFreqDriver.class);
        
        job1.setMapperClass(BrandPairFreqMapper1.class);
        
        job1.setNumReduceTasks(3);
        job1.setPartitionerClass(BrandPairFreqPartitioner.class);
        
        job1.setReducerClass(BrandPairFreqReducer1.class);
        
        
        //creating a JobConf object and assigning a job name
        JobConf job2 = new JobConf(getConf(),BrandPairFreqDriver.class);
        job2.setJobName("asg1_Job2");
        
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);
        
        job2.setMapperClass(BrandPairFreqMapper2.class);
        job2.setReducerClass(BrandPairFreqReducer2.class);
                

        //the hdfs input and output directory
        FileInputFormat.addInputPath(job1, new Path("asg1_input")); //new Path(args[0]));        
        FileOutputFormat.setOutputPath(job1,new Path("asg1_output1")); //new Path(args[1]));
        JobClient.runJob(job1);
        
        FileInputFormat.addInputPath(job2, new Path("asg1_output1")); //new Path(args[0]));        
        FileOutputFormat.setOutputPath(job2,new Path("asg1_output2")); //new Path(args[1]));
        JobClient.runJob(job2);

        return 0;

	}

	public static void main(String[] args) throws Exception{

		int res = ToolRunner.run(new Configuration(), new BrandPairFreqDriver(), args);
		System.exit(res);

	}
} 
