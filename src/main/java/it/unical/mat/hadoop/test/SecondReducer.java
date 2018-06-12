package it.unical.mat.hadoop.test;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SecondReducer extends Reducer<LongWritable, Text, LongWritable, Text>{

	@Override
	protected void reduce(LongWritable key, Iterable<Text> arg1,
			Reducer<LongWritable, Text, LongWritable, Text>.Context context) throws IOException, InterruptedException {
		
		String  val1=null;
		String  val2=null;
		String  val3=null;
		
		for(Text t:arg1) {
			String[] split = t.toString().split("\t");

			if(split[0].equals("me-asp")) {
				val1=split[1];
			}else {
				if(split[0].equals("idlv+-s")) {
					val2=split[1];					
				}else {
					//case of: lp2normal+clasp
					val3=split[1];
				}
				
			}

		}
		
		context.write(key, new Text(val1+"\t"+val2+"\t"+val3));
	}
	
	private static final String MAX_VAL=(new Float(Float.MAX_VALUE)).toString();
	
	private String setValueOrDefault(String s) {
		if(s.equals(MAX_VAL)) {
			return " - ";
		}else {
			return s;
		}
	}
	
	
	
	

}
