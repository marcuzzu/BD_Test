package it.unical.mat.hadoop.test;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class PartitionerTest extends Partitioner<FloatWritable, Text>{

	

	@Override
	public int getPartition(FloatWritable key, Text val, int arg2) {


		String[] split = val.toString().split(",");

		return Integer.parseInt(split[1]);


	}
	
	
	private int customHash(String str) {
		int hash = 127;
		for (int i = 0; i < str.length(); i++) {
		    hash = hash*521 + str.charAt(i);
		}
		return hash;
	}

}
