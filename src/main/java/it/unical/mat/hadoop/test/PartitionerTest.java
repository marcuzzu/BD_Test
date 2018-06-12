package it.unical.mat.hadoop.test;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class PartitionerTest extends Partitioner<FloatWritable, Text>{

	

	@Override
	public int getPartition(FloatWritable key, Text val, int arg2) {
	

		if(val.toString().equals("me-asp")) {
			return 0;
		}else {
			if(val.toString().equals("idlv+-s")) {
				return 1;					
			}else {
				//case of: lp2normal+clasp
				return 2;
			}
			
		}

	}
	
	
	private int customHash(String str) {
		int hash = 127;
		for (int i = 0; i < str.length(); i++) {
		    hash = hash*521 + str.charAt(i);
		}
		return hash;
	}

}
