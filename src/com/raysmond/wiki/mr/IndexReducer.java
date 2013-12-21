package com.raysmond.wiki.mr;

import java.io.IOException;
import java.util.Iterator;
import java.util.TreeMap;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import com.raysmond.wiki.writable.IndexList;
import com.raysmond.wiki.writable.WordIndex;

/**
 * IndexReducer class
 * The reducer receives a key word and a list of it's indexes and then sort the indexes by page ID
 * and save the result into HBase finally.
 * 
 * @author Raysmond, Junshi Guo
 * 
 */
public class IndexReducer extends TableReducer<Text, WordIndex, Text> {
	private TreeMap<String, WordIndex> map;

	@Override
	public void reduce(Text key, Iterable<WordIndex> values, Context context)
			throws IOException, InterruptedException {
		map = new TreeMap<String, WordIndex>();

		Iterator<WordIndex> it = values.iterator();
		while (it.hasNext()) {
			WordIndex index = it.next();
			String aid = index.getArticleId();
			// TODO why position list is empty?
			System.out.println(aid + ":" + index.getPositions());
			if (map.get(aid) == null) {
				// deep copy the object, or the values will all be same
				map.put(aid, new WordIndex(new String(aid), index.getPositions()));
			}
		}

	    System.out.println(key.toString() + ": " +(new IndexList(map)).toString());

		Put put = new Put(key.getBytes());
		put.add(Bytes.toBytes("content"), Bytes.toBytes("index"), Bytes.toBytes((new IndexList(map)).toString()));
		context.write(new Text(key), put);
	}
}
