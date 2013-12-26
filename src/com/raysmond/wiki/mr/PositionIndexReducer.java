package com.raysmond.wiki.mr;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;

import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.Text;

import com.raysmond.wiki.writable.IndexList;
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

	@Override
	public void reduce(Text key, Iterable<PositionIndex> values, Context context)
			throws IOException, InterruptedException {
		IndexList<PositionIndex> list = new IndexList<PositionIndex>();

		Iterator<PositionIndex> it = values.iterator();
		while (it.hasNext()) {
			list.add(new PositionIndex(it.next()));
		}
		
		// Sort posting list by article id
		Collections.sort(list);
		
		context.write(key, list);

	}
}
