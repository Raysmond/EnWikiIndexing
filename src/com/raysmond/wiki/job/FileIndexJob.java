package com.raysmond.wiki.job;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

import com.raysmond.wiki.mr.FileIndexingMapper;
import com.raysmond.wiki.mr.FileIndexingReducer;
import com.raysmond.wiki.mr.XmlInputFormat;
import com.raysmond.wiki.writable.WeightingIndex;
import com.raysmond.wiki.writable.WeightingIndexList;

/**
 * FileIndexJob
 * Map all pages into indexes with term weighting and reduce them by word, 
 * and finally store the result into HDFS.
 * 
 * @author Raysmond
 *
 */
public class FileIndexJob extends IndexJob {

	public static void main(String[] args) throws Exception {
		if (args.length < 2) {
			System.err.println("Both input and output path must be set!");
			System.exit(1); 
		}
		FileIndexJob job = new FileIndexJob();
		job.setInputPath(args[0]);
		job.setOutputPath(args[1]);
		job.call();
	}

	@Override
	public Job initialize(Configuration conf) throws IOException {
		// zookeeper
		// conf.set("mapred.job.tracker", "localhost:9001");
		// conf.set("hbase.zookeeper.quorum", "localhost");
		// conf.set("hbase.zookeeper.property.clientPort", "2222");

		// input format
		// conf.set(XmlInputFormat.START_TAG_KEY, "<page>");
		// conf.set(XmlInputFormat.END_TAG_KEY, "</page>");

		// Job initialization
		Job job = new Job(conf, "File indexing job");
		job.setJarByClass(FileIndexJob.class);

		job.setMapperClass(FileIndexingMapper.class);
		job.setReducerClass(FileIndexingReducer.class);

		// job.setNumReduceTasks(1);

		// Map output
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(WeightingIndex.class);
		job.setPartitionerClass(HashPartitioner.class);

		// Input and output
		job.setInputFormatClass(XmlInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputValueClass(WeightingIndexList.class);

		return job;
	}
}
