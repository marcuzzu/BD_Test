package it.unical.mat.hadoop.test;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FirstReducer extends Reducer<FloatWritable, Text, LongWritable,Text>{


	private long count=0;

	@Override
	protected void reduce(FloatWritable key, Iterable<Text> value, Reducer<FloatWritable, Text,
			LongWritable, Text>.Context context)
			throws IOException, InterruptedException {

			for(Text t:value) {
				
				String[] split = t.toString().split(",");
				
				context.write(new LongWritable(count++),new Text(key+","+split[0]));				
				
			}
			
			
		}
			
		
}
	
	
	
