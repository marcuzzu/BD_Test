package it.unical.mat.hadoop.test;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SecondMapper extends Mapper<LongWritable, Text	, LongWritable, Text>{

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, LongWritable, Text>.Context context)throws IOException, InterruptedException {
		
		
		System.out.println("KEY="+key);
		
		context.write(key, value);
	}

	
	
	
	
	
}
