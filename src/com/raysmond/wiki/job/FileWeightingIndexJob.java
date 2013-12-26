package com.raysmond.wiki.job;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

import com.raysmond.wiki.mr.FileIndexingMapper;
import com.raysmond.wiki.mr.FileIndexingReducer;
import com.raysmond.wiki.util.CounterTag;
import com.raysmond.wiki.util.XmlInputFormat;
import com.raysmond.wiki.writable.IndexList;
import com.raysmond.wiki.writable.WeightingIndex;

/**
 * FileIndexJob
 * Map all pages into indexes with term weighting and reduce them by word, 
 * and finally store the result into HDFS.
 * 
 * @author Raysmond
 *
 */
public class FileWeightingIndexJob extends IndexJob {

	private long pagesCount = 0;
	
	public static void main(String[] args) throws Exception {
		if (args.length < 2) {
			System.err.println("Both input and output path must be set!");
			System.exit(1); 
		}
		FileWeightingIndexJob job = new FileWeightingIndexJob();
		job.setInputPath(args[0]);
		job.setOutputPath(args[1]);
		if(args.length>=3){
			job.setReducerNum(Integer.parseInt(args[2]));
		}
		if(args.length>=4)
			job.setPagesCount(Long.parseLong(args[3]));
		
		job.call();
	}
	
	public long getPagesCount(){
		return pagesCount;
	}
	
	public void setPagesCount(long count){
		pagesCount = count;
	}
	
	@Override
	public Job initialize(Configuration conf) throws IOException {
		conf.setLong("total_pages", getPagesCount());
		
		// Job initialization
		Job job = new Job(conf, "File indexing job");
		job.setJarByClass(FileWeightingIndexJob.class);

		job.setMapperClass(FileIndexingMapper.class);
		job.setReducerClass(FileIndexingReducer.class);

		// Map output
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(WeightingIndex.class);
		job.setPartitionerClass(HashPartitioner.class);

		// Input and output
		job.setInputFormatClass(XmlInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputValueClass(IndexList.class);
		
		return job;
	}
	
}
