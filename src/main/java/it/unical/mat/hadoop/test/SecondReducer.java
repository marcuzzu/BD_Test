package it.unical.mat.hadoop.test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SecondReducer extends Reducer<LongWritable, Text, LongWritable, Text>{

	private Map<String, Integer> mapAlgoPartitiner;
	
	@Override
	protected void setup(Reducer<LongWritable, Text, LongWritable, Text>.Context context)
			throws IOException, InterruptedException {
		
		// init the reducer to handle all the the different type of algo
		mapAlgoPartitiner=new HashMap<String, Integer>();
		
		Configuration configuration = context.getConfiguration();
		
		int totAlgo=Integer.parseInt(configuration.get(MainApplication.NUMBER_ALGO));
		
		String fileHeader="";
		
		for(int i=0;i<totAlgo;i++) {
			String nameAlgo=configuration.get(MainApplication.NAME_ALGO_NUM+i);
			mapAlgoPartitiner.put(nameAlgo, i);
			
			fileHeader+=nameAlgo+"\t";
		}
		
		
		
		context.write(new LongWritable(new Long("-1")), new Text(fileHeader));
	}
	
	@Override
	protected void reduce(LongWritable key, Iterable<Text> arg1,
			Reducer<LongWritable, Text, LongWritable, Text>.Context context) throws IOException, InterruptedException {
		
		String[] values = new String[mapAlgoPartitiner.values().size()];

		
		for(Text t:arg1) {
			String[] split = t.toString().split(",");

			values[mapAlgoPartitiner.get(split[1])]=setValueOrDefault(split[0]);


		}
		
		context.write(key, new Text(values[0]+"\t"+values[1]+"\t"+values[2]));
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
