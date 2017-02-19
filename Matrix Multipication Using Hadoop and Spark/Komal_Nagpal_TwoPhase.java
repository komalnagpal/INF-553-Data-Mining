// Two phase matrix multiplication in Hadoop MapReduce
// Template file for homework #1 - INF 553 - Spring 2017
// - Wensheng Wu

import java.io.IOException;

// add your import statement here if needed
// you can only import packages from java.*;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.util.*;

public class TwoPhase {

    // mapper for processing entries of matrix A
    public static class PhaseOneMapperA 
	extends Mapper<LongWritable, Text, Text, Text> {
	
	private Text outKey = new Text();
	private Text outVal = new Text();

	public void map(LongWritable key, Text value, Context context)
	    throws IOException, InterruptedException {
	    
	    // fill in your code
	    	String[] input = value.toString().split(",");
	    	String matrixName = "A";
	    	int rowIndex = Integer.parseInt(input[0]);
	    	int colIndex = Integer.parseInt(input[1]);
	    	outKey.set(Integer.toString(colIndex));
            outVal.set(matrixName + "," + Integer.toString(rowIndex) + "," + input[2].toString() );
	    	context.write(outKey,outVal);
	}

    }

    // mapper for processing entries of matrix B
    public static class PhaseOneMapperB
	extends Mapper<LongWritable, Text, Text, Text> {
	
	private Text outKey = new Text();
	private Text outVal = new Text();

	public void map(LongWritable key, Text value, Context context)
	    throws IOException, InterruptedException {
	    
	    // fill in your code
	    	String[] input = value.toString().split(",");
	    	String matrixName = "B";
	    	int rowIndex = Integer.parseInt(input[0]);
	    	int colIndex = Integer.parseInt(input[1]);
	    	outKey.set(Integer.toString(rowIndex));
            outVal.set(matrixName + "," + Integer.toString(colIndex) + "," + input[2].toString() );
	    	context.write(outKey,outVal);


	}
    }

    public static class PhaseOneReducer
	extends Reducer<Text, Text, Text, Text> {

	private Text outKey = new Text();
	private Text outVal = new Text();

	public void reduce(Text key, Iterable<Text> values, Context context) 
	    throws IOException, InterruptedException {
	    
	    // fill in your code
	    	ArrayList<String> matrixA = new ArrayList<String>();
	    	ArrayList<String> matrixB = new ArrayList<String>();
	    	for (Text text :values)
	    	{
	    		String value = text.toString();
	    		if (value.startsWith("A"))
	    			matrixA.add(value);
	    		else
	    			matrixB.add(value);
	    	}
	    	for (String rowInA: matrixA)
	    	{
	    		String[] rowValuesInA = rowInA.split(",");
	    		for (String colInB: matrixB)
	    		{
	    			String[]  colValuesInB = colInB.split(",");
	    			outKey.set(rowValuesInA[1] + "," + colValuesInB[1]);
	    			outVal.set(Integer.toString(Integer.parseInt(rowValuesInA[2]) * Integer.parseInt(colValuesInB[2])));
	    			context.write(outKey,outVal);
	    		}
	    	}
	}

    }

    public static class PhaseTwoMapper 
	extends Mapper<Text, Text, Text, Text> {
	
	private Text outKey = new Text();
	private Text outVal = new Text();

	public void map(Text key, Text value, Context context)
	    throws IOException, InterruptedException {

	    // fill in your code
	    	outKey.set(key);
	    	outVal.set(value);
	    	context.write(outKey,outVal);

	}
    }

    public static class PhaseTwoReducer 
	extends Reducer<Text, Text, Text, Text> {
	
	private Text outKey = new Text();
	private Text outVal = new Text();

	public void reduce(Text key, Iterable<Text> values, Context context)
	    throws IOException, InterruptedException {
	 
	    // fill in your code
	    	int sum = 0;
	    	for (Text value: values)
	    	{
	    		sum+= Integer.parseInt(value.toString());

	    	}
	    	outKey.set(key);
	    	outVal.set(Integer.toString(sum));
	    	context.write(outKey,outVal);

	}
    }


    public static void main(String[] args) throws Exception {
	Configuration conf = new Configuration();

	Job jobOne = Job.getInstance(conf, "phase one");

	jobOne.setJarByClass(TwoPhase.class);

	jobOne.setOutputKeyClass(Text.class);
	jobOne.setOutputValueClass(Text.class);

	jobOne.setReducerClass(PhaseOneReducer.class);

	MultipleInputs.addInputPath(jobOne,
				    new Path(args[0]),
				    TextInputFormat.class,
				    PhaseOneMapperA.class);

	MultipleInputs.addInputPath(jobOne,
				    new Path(args[1]),
				    TextInputFormat.class,
				    PhaseOneMapperB.class);

	Path tempDir = new Path("temp");

	FileOutputFormat.setOutputPath(jobOne, tempDir);
	jobOne.waitForCompletion(true);


	// job two
	Job jobTwo = Job.getInstance(conf, "phase two");
	

	jobTwo.setJarByClass(TwoPhase.class);

	jobTwo.setOutputKeyClass(Text.class);
	jobTwo.setOutputValueClass(Text.class);

	jobTwo.setMapperClass(PhaseTwoMapper.class);
	jobTwo.setReducerClass(PhaseTwoReducer.class);

	jobTwo.setInputFormatClass(KeyValueTextInputFormat.class);

	FileInputFormat.setInputPaths(jobTwo, tempDir);
	FileOutputFormat.setOutputPath(jobTwo, new Path(args[2]));
	
	jobTwo.waitForCompletion(true);
	
	FileSystem.get(conf).delete(tempDir, true);
	
    }
}
