package it.unical.mat.hadoop.test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;


/**
 * 
 * @author Marco
 *
 *	the idea is to use 2 jobs the first job read the input, clean all the data and output only: time and the algorithmName
 *after a custom partitioner send the data t a reducer that will take care of all records of the  same algorithm, so will print 
 *a file for each algorithm whit the sorted list of time.
 *
 *The second Job will take all the input e sort it by line number, and print it as trasposed table (the time of eahc algorithm is already sorted).
 *
 *To make the system independent by the number of the algorithm I provide a ConfigJob that print a file whit the name of the algorithm and a 
 *number to order it, so at the end of the ConfigJob the system read the output and store this information into the hadoop configuration.
 *the configuration will be used by the first mapper and the second reducer.
 *
 */
public class MainApplication{

//	.\hadoop jar .\exe\MR_Task.jar .\exe\hadIn .\exe\hadOut

	public static final String NUMBER_ALGO="number_algo";
	
	public static final String NAME_ALGO_NUM="name_algo";
	
	
	private static final String CONF_FOLDER="/conf";
	private static final String STAGING_FOLDER="/temp";
	private static final String FINAL_FOLDER="/final";
	

	
	
	public static void main(String[] args) throws Exception {
		
		System.out.println(args[0]);
		System.out.println(args[1]);
		
		Configuration configuration = new Configuration();

		/************* config JOB *******/
		
		Job jobConf=Job.getInstance(configuration, "ConfigJob");
		
		
		jobConf.setOutputKeyClass(Text.class);
		jobConf.setOutputValueClass(IntWritable.class);
		
		jobConf.setMapperClass(ConfigMapper.class);
		jobConf.setReducerClass(ConfigReducer.class);
		
		jobConf.setInputFormatClass(TextInputFormat.class);
		jobConf.setOutputFormatClass(TextOutputFormat.class);
		
		jobConf.setNumReduceTasks(1);
		jobConf.setPartitionerClass(PartitionerTest.class);
		
		FileInputFormat.setInputPaths(jobConf, new Path(args[0]));
		FileOutputFormat.setOutputPath(jobConf, new Path(args[1]+CONF_FOLDER));
		
		jobConf.waitForCompletion(true);

		
		/*************	retrive the configuration INFO	************/
		readFromTempFile(args[1]+CONF_FOLDER, configuration);
		
		
		/*************	start first JOB	****************/

		Job job=Job.getInstance(configuration, "FirstJob");
		
		job.setNumReduceTasks(Integer.parseInt(configuration.get(NUMBER_ALGO)));
		
		job.setOutputKeyClass(FloatWritable.class);
		job.setOutputValueClass(Text.class);
		
		job.setMapperClass(FirstMapper.class);
		job.setReducerClass(FirstReducer.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setPartitionerClass(PartitionerTest.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]+STAGING_FOLDER));
		
		job.waitForCompletion(true);
		
		/*************	start second JOB	****************/

		Job job2=Job.getInstance(configuration, "SecondJob");
		
		job2.setOutputKeyClass(LongWritable.class);
		job2.setOutputValueClass(Text.class);
		
		job2.setMapperClass(SecondMapper.class);
		job2.setReducerClass(SecondReducer.class);
		
		job2.setInputFormatClass(TextInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);
		

		
		FileInputFormat.setInputPaths(job2, new Path(args[1]+STAGING_FOLDER));
		FileOutputFormat.setOutputPath(job2, new Path(args[1]+FINAL_FOLDER));
		
		job2.waitForCompletion(true);

	}
	
	
	private static  void readFromTempFile(String baseConfFolder,Configuration conf) throws Exception {
		
		FileReader fr = new FileReader(baseConfFolder+"/part-r-00000");
		BufferedReader br = new BufferedReader(fr);

		String sCurrentLine;
		int count=0;

		while ((sCurrentLine = br.readLine()) != null) {
			String[] split = sCurrentLine.split("\t");
			conf.set(NAME_ALGO_NUM+count++, split[0]);
		}
		
		conf.set(NUMBER_ALGO, count+"");
	}
		



}
