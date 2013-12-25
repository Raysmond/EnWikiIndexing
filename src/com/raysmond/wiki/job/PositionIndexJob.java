package com.raysmond.wiki.job;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

import com.raysmond.wiki.mr.PositionIndexMapper;
import com.raysmond.wiki.mr.PositionIndexReducer;
import com.raysmond.wiki.util.XmlInputFormat;
import com.raysmond.wiki.writable.IndexList;
import com.raysmond.wiki.writable.PositionIndex;

/**
 * PositionIndexJob
 * Create an inverted indexes with full positions in article.
 * 
 * @author Raysmond
 */
public class PositionIndexJob extends IndexJob {

	public static void main(String[] args) throws Exception {
		if (args.length < 2) {
			System.err.println("Both input and output path must be set!");
			System.exit(1); 
		}
		PositionIndexJob job = new PositionIndexJob();
		job.setInputPath(args[0]);
		job.setOutputPath(args[1]);
		if(args.length>=3){
			job.setReducerNum(Integer.parseInt(args[2]));
		}
		job.call();
	}

	@Override
	public Job initialize(Configuration conf) throws IOException {
		// Job initialization
		Job job = new Job(conf, "Position indexing job");
		job.setJarByClass(PositionIndexJob.class);

		job.setMapperClass(PositionIndexMapper.class);
		job.setReducerClass(PositionIndexReducer.class);

		// Map output
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(PositionIndex.class);
		job.setPartitionerClass(HashPartitioner.class);

		// Input and output
		job.setInputFormatClass(XmlInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputValueClass(IndexList.class);

		return job;
	}
}
