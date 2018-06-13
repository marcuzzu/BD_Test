package it.unical.mat.hadoop.test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ConfigReducer extends Reducer<Text, IntWritable, Text, IntWritable>{

	List<String> algoName=new ArrayList();

	@Override
	protected void reduce(Text arg0, Iterable<IntWritable> arg1,
			Reducer<Text, IntWritable, Text, IntWritable>.Context arg2) throws IOException, InterruptedException {
		
		algoName.add(arg0.toString());
		
	}
	
	@Override
	protected void cleanup(Reducer<Text, IntWritable, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		

		int count=0;
		
		for(String s:algoName) {
			context.write(new Text(s), new IntWritable(count++));
		}
		
	}
	
	
}
