package edu.cs.utexas.HadoopEx;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import edu.cs.utexas.HadoopEx.IntArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<Object, Text, Text, IntArrayWritable> {

	// Create array of IntWritable values
	// private final IntWritable[] valueArr = new IntWritable[2];
	// Create a counter and initialize with 1
	// private final IntArrayWritable counter = new IntArrayWritable(valueArr);
	// Create a hadoop text object to store words
	private Text word = new Text();

	public void map(Object key, Text value, Context context) 
			throws IOException, InterruptedException {

		String line = value.toString();
		StringTokenizer itr = new StringTokenizer(line, ",");
		for(int i = 0; i < 4 && itr.hasMoreTokens(); i++){
			itr.nextToken();
		}
		word.set(itr.nextToken());

		for(int i = 0; i < 6 && itr.hasMoreTokens(); i++){
			itr.nextToken();
		}

		float departureDelay = 0;
		try{
			departureDelay = Float.parseFloat(itr.nextToken());
		} catch (Exception e){
			return;
		}
		
		FloatWritable[] floatArr = new FloatWritable[2];
		//11 - Departure delay
		floatArr[0] = new FloatWritable(departureDelay); // departure delay
		floatArr[1] = new FloatWritable(1); //number of flights

		context.write(word, new IntArrayWritable(floatArr));
	}
}