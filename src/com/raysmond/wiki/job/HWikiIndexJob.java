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
import com.raysmond.wiki.mr.HIndexMapper;
import com.raysmond.wiki.mr.HIndexReducer;
import com.raysmond.wiki.mr.XmlInputFormat;
import com.raysmond.wiki.writable.WordIndex;

public class HWikiIndexJob {

	private String inputPath;

	private String outputPath;
	
	public void call() throws Exception {
		//create table
		String tableName = "wikiIndex";
		HWikiIndexJob.createHBaseTable(tableName);
		//configure mapreduce
		Configuration conf = new Configuration();
		//conf.set("mapred.job.tracker", "localhost:9001");
		//conf.set("hbase.zookeeper.quorum", "localhost");
		//conf.set("hbase.zookeeper.property.clientPort", "2222");
		conf.set(TableOutputFormat.OUTPUT_TABLE, tableName);
		//input format
		//conf.set(XmlInputFormat.START_TAG_KEY, "<page>");
		//conf.set(XmlInputFormat.END_TAG_KEY, "</page>");
		
		Job job = new Job(conf, "Wiki Index by HBase");
		job.setJarByClass(HWikiIndexJob.class);
		//Mapper and Reducer
		job.setMapperClass(HIndexMapper.class);
		job.setReducerClass(HIndexReducer.class);
		//job.setNumReduceTasks(1);
		//output type
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(WordIndex.class);
		
		TableMapReduceUtil.initTableReducerJob("wikiIndex", HIndexReducer.class, job); 
		
		//output format
		job.setInputFormatClass(XmlInputFormat.class);
		job.setOutputFormatClass(TableOutputFormat.class);		
		// Input path
		FileInputFormat.addInputPath(job, new Path(getInputPath()));
		//if (getOutputPath() != null)
		//	FileOutputFormat.setOutputPath(job, new Path(getOutputPath()));
		
		
		System.exit(job.waitForCompletion(true)?0:1);
	}

	// 创建 HBase 数据表
	public static void createHBaseTable(String tableName) throws IOException {
		// 创建表描述
		HTableDescriptor htd = new HTableDescriptor(tableName);
		// 创建列族描述
		HColumnDescriptor col = new HColumnDescriptor("content");
		htd.addFamily(col);

		// 配置 HBase
		Configuration conf = HBaseConfiguration.create();

		//conf.set("hbase.zookeeper.quorum", "localhost");
		//conf.set("hbase.zookeeper.property.clientPort", "2222");
		HBaseAdmin hAdmin = new HBaseAdmin(conf);

		if (hAdmin.tableExists(tableName)) {
			System.out.println("该数据表已经存在，正在重新创建。");
			hAdmin.disableTable(tableName);
			hAdmin.deleteTable(tableName);
		}

		System.out.println("创建表：" + tableName);
		hAdmin.createTable(htd);
	}

	// getters and setters

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
