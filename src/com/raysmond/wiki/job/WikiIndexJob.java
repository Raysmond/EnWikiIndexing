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
import com.raysmond.wiki.mr.IndexMapper;
import com.raysmond.wiki.mr.IndexReducer;
import com.raysmond.wiki.mr.XmlInputFormat;
import com.raysmond.wiki.writable.WordIndex;

public class WikiIndexJob {

	private String inputPath;

	private String outputPath;
	
//	public static void main(String[] args) throws Exception {
//		WikiIndexJob job = new WikiIndexJob();
//
//		if (args.length >= 1)
//			job.setInputPath(args[0]);
//		if (args.length >= 2)
//			job.setOutputPath(args[1]);
//
//		job.call();
//		//job.getRunningJob().waitForCompletion();
//	}
	
	public void call() throws Exception {
		//create table
		String tableName = "wikiIndex";
		WikiIndexJob.createHBaseTable(tableName);
		
		//configure mapreduce
		Configuration conf = new Configuration();
		
		// zookeeper
		//conf.set("mapred.job.tracker", "localhost:9001");
		//conf.set("hbase.zookeeper.quorum", "localhost");
		//conf.set("hbase.zookeeper.property.clientPort", "2222");
		
		conf.set(TableOutputFormat.OUTPUT_TABLE, tableName);
		//input format
		//conf.set(XmlInputFormat.START_TAG_KEY, "<page>");
		//conf.set(XmlInputFormat.END_TAG_KEY, "</page>");
		
		// Job initialization
		Job job = new Job(conf, "Wiki Index by HBase");
		job.setJarByClass(WikiIndexJob.class);
		
		//Mapper and Reducer
		job.setMapperClass(IndexMapper.class);
		job.setReducerClass(IndexReducer.class);
		
		//job.setNumReduceTasks(1);
		
		// Map output
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(WordIndex.class);
		
		
		TableMapReduceUtil.initTableReducerJob("wikiIndex", IndexReducer.class, job); 
		
		// Input and output format
		job.setInputFormatClass(XmlInputFormat.class);
		job.setOutputFormatClass(TableOutputFormat.class);	
		
		// Input path
		FileInputFormat.addInputPath(job, new Path(getInputPath()));
		
		 long startTime = System.currentTimeMillis();
		job.waitForCompletion(true);
		System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
	}

	/**
	 * Create table in HBase
	 * @param tableName
	 * @throws IOException
	 */
	public static void createHBaseTable(String tableName) throws IOException {
		HTableDescriptor htd = new HTableDescriptor(tableName);
		HColumnDescriptor col = new HColumnDescriptor("content");
		htd.addFamily(col);

		Configuration conf = HBaseConfiguration.create();
		//conf.set("hbase.zookeeper.quorum", "localhost");
		//conf.set("hbase.zookeeper.property.clientPort", "2222");
		HBaseAdmin hAdmin = new HBaseAdmin(conf);

		if (hAdmin.tableExists(tableName)) {
			System.out.println("The table already exists, begin to recreate it...");
			hAdmin.disableTable(tableName);
			hAdmin.deleteTable(tableName);
		}

		hAdmin.createTable(htd);
		System.out.println("Table " + tableName + " has been created.");
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
