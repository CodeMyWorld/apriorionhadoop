package MapReduce;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
public class AprioriReducer  extends Reducer<Text, IntWritable, Text, IntWritable>{
	public void reduce(Text key, Iterable<IntWritable> values, Context context) 
	throws IOException, InterruptedException{
		int support = 0;
		for(IntWritable supportIntWritable : values){
			support += supportIntWritable.get();
		}
		context.write(key, new IntWritable(support));
	}
}
