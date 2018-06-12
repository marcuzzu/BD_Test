package it.unical.mat.hadoop.test;

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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;


public class MainApplication extends Configured implements Tool{

//	.\hadoop jar .\exe\MR_Task.jar .\exe\hadIn .\exe\hadOut

	
	public int run(String[] args) throws Exception {

		System.out.println(args[0]);
		System.out.println(args[1]);
		

	    JobControl jobControl = new JobControl("jobChain"); 
	    Configuration conf1 = getConf();

	    Job job1 = Job.getInstance(conf1);  
	    job1.setJobName("Job1");

	    FileInputFormat.setInputPaths(job1, new Path(args[0]));
	    TextOutputFormat.setOutputPath(job1, new Path(args[1] + "/temp"));

		job1.setOutputKeyClass(FloatWritable.class);
		job1.setOutputValueClass(Text.class);
	    
	    job1.setMapperClass(MapperJoin.class);
	    job1.setReducerClass(ReduceJoin.class);

		job1.setPartitionerClass(PartitionerTest.class);
		
		
		job1.setNumReduceTasks(3);

	    ControlledJob controlledJob1 = new ControlledJob(conf1);
	    controlledJob1.setJob(job1);

	    jobControl.addJob(controlledJob1);


	    Configuration conf2 = getConf();

	    Job job2 = Job.getInstance(conf2);
//	    job2.setJarByClass(WordCombined.class);
	    job2.setJobName("secondJob");

	    
	    TextInputFormat.setInputPaths(job2, new Path(args[1] + "/temp"));
	    FileOutputFormat.setOutputPath(job2, new Path(args[1] + "/final"));

	    job2.setMapperClass(SecondMapper.class);
	    job2.setReducerClass(SecondReducer.class);
	    

	    
	    job2.setOutputKeyClass(LongWritable.class);
	    job2.setOutputValueClass(Text.class);

	    ControlledJob controlledJob2 = new ControlledJob(conf2);
	    controlledJob2.setJob(job2);

	    // make job2 dependent on job1
	    controlledJob2.addDependingJob(controlledJob1); 
	    // add the job to the job control
	    jobControl.addJob(controlledJob2);

	    job2.setNumReduceTasks(0);

	    Thread jobControlThread = new Thread(jobControl);
	    jobControlThread.start();

	    
	    
	   return (job2.waitForCompletion(true) ? 0 : 1);   

	  } 

	
	
	
	public static void main(String[] args) throws Exception { 
		int exitCode = ToolRunner.run(new MainApplication(), args);  
		
		System.out.println("END");
		System.exit(exitCode);
	}

}
