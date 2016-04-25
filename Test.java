
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;
import MapReduce.AprioriMapper;
import MapReduce.AprioriReducer;


 
public class Test extends Configured implements Tool{
	@Override
	public int run(String[] arg0) throws Exception{
		Job job = new Job();
		job.setJarByClass(Test.class);
		job.setJobName("Apriori");
		
		FileInputFormat.addInputPath(job, new Path(arg0[0]));
		FileOutputFormat.setOutputPath(job, new Path(arg0[1]));
		
	
		job.setReducerClass(AprioriReducer.class);
		job.setMapperClass(AprioriMapper.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.waitForCompletion(true);
		return 0;
	}
	
    public static void main(String[] args) throws Exception {  
        int res = ToolRunner.run(new Configuration(), new Test(), args);  
        System.exit(res);  
    }  

}
