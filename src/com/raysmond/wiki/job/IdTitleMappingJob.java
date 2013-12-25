package com.raysmond.wiki.job;
import com.raysmond.wiki.mr.*;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import com.raysmond.wiki.util.XmlInputFormat;

// TODO
public class IdTitleMappingJob extends IndexJob {

	public static void main(String[] args) throws Exception {
		if (args.length < 2) {
			System.err.println("Both input and output path must be set!");
			System.exit(1);
		}
		IdTitleMappingJob job = new IdTitleMappingJob();
		job.setInputPath(args[0]);
		job.setOutputPath(args[1]);
		job.setReducerNum(0);
		job.call();
	}

	@Override
	public Job initialize(Configuration conf) throws IOException {
		// Job initialization
		Job job = new Job(conf, "Id Title Mapping job");
		job.setJarByClass(IdTitleMappingJob.class);

		job.setMapperClass(IdTitleMapper.class);
		//job.setReducerClass(IdTitleReducer.class);

		// Map output
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setPartitionerClass(HashPartitioner.class);

		// Input and output
		job.setInputFormatClass(XmlInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputValueClass(Text.class);

		return job;
	}

}
