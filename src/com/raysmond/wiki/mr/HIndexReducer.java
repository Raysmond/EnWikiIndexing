package com.raysmond.wiki.mr;

import java.io.IOException;
import java.util.Iterator;
import java.util.TreeMap;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import com.raysmond.wiki.writable.IndexList;
import com.raysmond.wiki.writable.WordIndex;

/**
 * IndexReducer class
 * 
 * @author Raysmond
 * 
 */
//public class HIndexReducer extends TableReducer<Text, WordIndex, Text, IndexList> {
public class HIndexReducer extends TableReducer<Text, IntWritable, NullWritable> {
	private TreeMap<String, WordIndex> map;

	protected void reduce(Text key, Iterator<WordIndex> values,Context context)
			throws IOException, InterruptedException {
		map = new TreeMap<String, WordIndex>();

		while (values.hasNext()) {
			WordIndex index = values.next();
			String aid = index.getArticleId();
			if (map.get(aid) == null) {
				// deep copy the object, or the values will all be same
				map.put(aid, new WordIndex(new String(aid), index.getPositions()));
			}
		}
		
		Put put = new Put(Bytes.toBytes(key.toString()));
		put.add(Bytes.toBytes("content"), Bytes.toBytes("index"),
				Bytes.toBytes((new IndexList(map)).toString()));
		context.write(NullWritable.get(), put);
	}

/*	private TreeMap<String, WordIndex> map;

	@Override
	public void reduce(Text key, Iterator<WordIndex> values,
			OutputCollector<Text, IndexList> output, Reporter reporter)
			throws IOException {
		map = new TreeMap<String, WordIndex>();

		while (values.hasNext()) {
			WordIndex index = values.next();
			String aid = index.getArticleId();
			if (map.get(aid) == null) {
				// deep copy the object, or the values will all be same
				map.put(aid, new WordIndex(new String(aid), index.getPositions()));
			}
		}
		output.collect(key, new IndexList(map));
	}*/
}
