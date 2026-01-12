package edu.cs.utexas.HadoopEx;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import edu.cs.utexas.HadoopEx.IntArrayWritable;


public class WordCountReducer extends  Reducer<Text, IntWritable, Text, IntArrayWritable> {

   public void reduce(Text text, Iterable<IntArrayWritable> values, Context context)
           throws IOException, InterruptedException {
	   
       float totalDelayDeparture = 0;
       float totalFlights = 0;
       
       for (IntArrayWritable value : values){
            FloatWritable[] floatWritables = (FloatWritable[]) value.get();
            totalDelayDeparture += floatWritables[0].get();
            totalFlights += floatWritables[1].get();
       }

       FloatWritable[] intWritables = new FloatWritable[] {
            new FloatWritable(totalDelayDeparture),
            new FloatWritable(totalFlights)
       };
       
       context.write(text, new IntArrayWritable(intWritables));
   }
}