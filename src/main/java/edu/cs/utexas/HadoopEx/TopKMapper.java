package edu.cs.utexas.HadoopEx;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.PriorityQueue;

import edu.cs.utexas.HadoopEx.IntArrayWritable;

import org.apache.log4j.Logger;


public class TopKMapper extends Mapper<Text, Text, Text, FloatWritable> {

	private Logger logger = Logger.getLogger(TopKMapper.class);


	private PriorityQueue<WordAndRatio> pq;

	public void setup(Context context) {
		pq = new PriorityQueue<>();

	}

	/**
	 * Reads in results from the first job and filters the topk results
	 *
	 * @param key
	 * @param value a float value stored as a string
	 */
	public void map(Text key, Text value, Context context)
			throws IOException, InterruptedException {

		String[] arr = value.toString().split(" ");
		float delay = Float.parseFloat(arr[1].toString());
		float count = Float.parseFloat(arr[2].toString());
		float ratio = delay/count;
		pq.add(new WordAndRatio(new Text(key), new FloatWritable(ratio)));

		if (pq.size() > 10) {
			pq.poll();
		}
	}

	public void cleanup(Context context) throws IOException, InterruptedException {

		while (pq.size() > 0) {
			WordAndRatio wordAndRatio = pq.poll();
			context.write(wordAndRatio.getWord(), wordAndRatio.getRatio());
			logger.info("TopKMapper PQ Status: " + pq.toString());
		}
	}

}