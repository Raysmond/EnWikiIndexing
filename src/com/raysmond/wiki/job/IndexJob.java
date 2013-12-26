package com.raysmond.wiki.job;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Abstract index job class
 * This class just do initialization work
 * 
 * @author Raysmond
 *
 */
abstract class IndexJob {
	private String inputPath;
	private String outputPath;
	private int reducerNum = 1;
	
	public void setReducerNum(int num){
		this.reducerNum = num;
	}
	public int getReducerNum(){
		return this.reducerNum;
	}
	public abstract Job initialize(Configuration conf) throws IOException;
	
	public void call() throws Exception {
		Configuration conf = new Configuration();
		
		Job job = initialize(conf);
		job.setNumReduceTasks(this.getReducerNum());
		
		// Input and output path
		FileInputFormat.addInputPath(job, new Path(getInputPath()));
		if (getOutputPath() != null)
            FileOutputFormat.setOutputPath(job, new Path(getOutputPath()));
		
		long startTime = System.currentTimeMillis();
		job.waitForCompletion(true);
		System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
		afterJob(job);
	}
	
	public void afterJob(Job job) throws IOException{
		// do something
	}

	public void setInputPath(String inputPath) {
		this.inputPath = inputPath;
	}

	public String getInputPath() {
		return inputPath;
	}

	public void setOutputPath(String outputPath) {
		this.outputPath = outputPath;
	}

	public String getOutputPath() {
		return outputPath;
	}
}
