package com.raysmond.wiki.job;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import com.raysmond.wiki.mr.PositionIndexMapper;
import com.raysmond.wiki.mr.PositionIndexReducer;
import com.raysmond.wiki.mr.WeightingIndexMapper;
import com.raysmond.wiki.mr.WeightingIndexReducer;
import com.raysmond.wiki.util.XmlInputFormat;
import com.raysmond.wiki.writable.WeightingIndex;

/**
 * WeightingIndexJob
 * Map all articles into word indexes with term weighting, reduce them by word as key,
 * finally store the result into HBase.
 * 
 * @author Raysmond, Junshi Guo
 *
 */
public class HBaseWeightingIndexJob extends IndexJob {

	public static void main(String[] args) throws Exception {
		if (args.length < 1) {
			System.err.println("Input path must be set!");
			System.exit(1); 
		}
		HBaseWeightingIndexJob job = new HBaseWeightingIndexJob();
		job.setInputPath(args[0]);
		job.call();
	}

	/**
	 * Create table in HBase
	 * 
	 * @param tableName
	 * @throws IOException
	 */
	public static void createHBaseTable(String tableName) throws IOException {
		HTableDescriptor htd = new HTableDescriptor(tableName);
		HColumnDescriptor col = new HColumnDescriptor("content");
		htd.addFamily(col);

		Configuration conf = HBaseConfiguration.create();
		// conf.set("hbase.zookeeper.quorum", "localhost");
		// conf.set("hbase.zookeeper.property.clientPort", "2222");
		HBaseAdmin hAdmin = new HBaseAdmin(conf);

		if (hAdmin.tableExists(tableName)) {
			System.out
					.println("The table already exists, begin to recreate it...");
			hAdmin.disableTable(tableName);
			hAdmin.deleteTable(tableName);
		}

		hAdmin.createTable(htd);
		System.out.println("Table " + tableName + " has been created.");
	}

	@Override
	public Job initialize(Configuration conf) throws IOException {
		// create table
		String tableName = "10300240065_31_wikiIndex";
		createHBaseTable(tableName);

		// zookeeper
		// conf.set("mapred.job.tracker", "localhost:9001");
		// conf.set("hbase.zookeeper.quorum", "localhost");
		// conf.set("hbase.zookeeper.property.clientPort", "2222");

		conf.set(TableOutputFormat.OUTPUT_TABLE, tableName);
		// input format
		// conf.set(XmlInputFormat.START_TAG_KEY, "<page>");
		// conf.set(XmlInputFormat.END_TAG_KEY, "</page>");

		// Job initialization
		Job job = new Job(conf, "Wiki Index by HBase");
		job.setJarByClass(HBaseWeightingIndexJob.class);

		// Mapper and Reducer
		job.setMapperClass(WeightingIndexMapper.class);
		job.setReducerClass(WeightingIndexReducer.class);

		// job.setNumReduceTasks(1);

		// Map output
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(WeightingIndex.class);

		TableMapReduceUtil.initTableReducerJob("10300240065_31_wikiIndex",
				WeightingIndexReducer.class, job);

		// Input and output format
		job.setInputFormatClass(XmlInputFormat.class);
		job.setOutputFormatClass(TableOutputFormat.class);
		return job;
	}
	
}
