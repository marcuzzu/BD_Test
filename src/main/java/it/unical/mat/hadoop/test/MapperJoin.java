package it.unical.mat.hadoop.test;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class MapperJoin extends Mapper<LongWritable, Text	, FloatWritable, Text>{

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, FloatWritable, Text>.Context context)
			throws IOException, InterruptedException {
		
			
		String filePAth=((FileSplit)context.getInputSplit()).getPath().getName();
				
		String[] split = value.toString().split("\t");
		
		// 0			1				2		3			4			5			6			7				8				9			10			11			12			13			14		15			16				17					
		// Solver	Executable.team	Problem	Instance	Exit-code	Exec-Status	Checker-out	Time(user+sys)	Memory-Usage	Time-Limit	Memory-Limit	Real	TrackFormula	Track	RESULT	SCORE	RESULT_IN_MARATHON	SCORE-MARATHON
		
//			System.out.println("algho="+split[0]+"   time="+split[11]);

		//solved
		if(split[0].equals("Solver")) {
			return;
		}
		
		if(split[14].equals("solved")) {
			try {
				float parseFloat = Float.parseFloat(split[11]);
				
				context.write(new FloatWritable(parseFloat), new Text(split[0]));
				
			}catch (Exception e) {
				System.err.println("********algho="+split[0]+"   time="+split[11]);				
			}			
		}else {		
			context.write(new FloatWritable(Float.MAX_VALUE), new Text(split[0]));
		}

	}
		

	
	
}
