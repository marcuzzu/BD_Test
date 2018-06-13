package it.unical.mat.hadoop.test;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ConfigMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	
	Set<String> algoSet=new HashSet<String>();

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {

		String[] split = value.toString().split("\t");
		
		//skip the header
		if(split[0].equals("Solver")) {
			return;
		}else {
			algoSet.add(split[0]);
		}	
	}
	
	@Override
	protected void cleanup(Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		
			for(String algoName:algoSet) {
				context.write(new Text(algoName), new IntWritable(0));
			}
	}
}
