package com.raysmond.wiki.mr;

import java.io.IOException;
import java.util.Iterator;
import java.util.TreeMap;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import com.raysmond.wiki.writable.PositionIndexList;
import com.raysmond.wiki.writable.PositionIndex;

/**
 * IndexReducer class
 * The reducer receives a key word and a list of it's indexes and then sort the indexes by page ID
 * and save the result into HBase finally.
 * 
 * @author Raysmond, Junshi Guo
 * 
 */
public class PositionIndexReducer extends TableReducer<Text, PositionIndex, Text> {
	private TreeMap<String, PositionIndex> map;

	@Override
	public void reduce(Text key, Iterable<PositionIndex> values, Context context)
			throws IOException, InterruptedException {
		map = new TreeMap<String, PositionIndex>();

		Iterator<PositionIndex> it = values.iterator();
		while (it.hasNext()) {
			PositionIndex index = it.next();
			String aid = index.getArticleId();
			if (map.get(aid) == null) {
				// deep copy the object, or the values will all be same
				map.put(aid, new PositionIndex(index));
			}
		}

	   // System.out.println(key.toString() + ": " +(new IndexList(map)).toString());
		Put put = new Put(Bytes.toBytes(key.toString()));
		put.add(Bytes.toBytes("content"), Bytes.toBytes("index"), Bytes.toBytes((new PositionIndexList(map)).toString()));
		context.write(new Text(key), put);
	}
}
