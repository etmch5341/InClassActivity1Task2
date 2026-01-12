package edu.cs.utexas.HadoopEx;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;

public class IntArrayWritable extends ArrayWritable implements Comparable<IntArrayWritable>{
    private float ratio; 

    public IntArrayWritable() {
        super(IntWritable.class);
    }

    public IntArrayWritable(FloatWritable[] values) {
        super(IntWritable.class, values);
        ratio = values[0].get()/values[1].get();
    }

    public float getRatio(){
        return this.ratio;
    } 

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("[ ");

        for (String s : super.toStrings())
        {
            sb.append(s).append(" ");
        }

        sb.append("]");
        return sb.toString();
    }

    @Override
    public int compareTo(IntArrayWritable o) {
        if(this.ratio > o.ratio){
            return 1;
        } else if (this.ratio < o.ratio){
            return -1;
        }
        return 0;
    }
}