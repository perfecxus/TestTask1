package com.sample;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*; 

public class Task1Mapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {
		String[] lineArray = value.toString().split(",");
		
		IntWritable year = new IntWritable(Integer.parseInt(lineArray[0].split("-")[2]));
		IntWritable temp = new IntWritable(Integer.parseInt(lineArray[2]));
		
		context.write(year, temp);
	}
}
