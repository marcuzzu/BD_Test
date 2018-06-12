package it.unical.mat.hadoop.test;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReduceJoin extends Reducer<FloatWritable, Text, Text, FloatWritable>{

	

	@Override
	protected void reduce(FloatWritable key, Iterable<Text> value, Reducer<FloatWritable, Text, Text, FloatWritable>.Context context)
			throws IOException, InterruptedException {

			for(Text t:value) {
				context.write(t,key);				
				
			}
			
			
		}
			
		
}
	
	
	
