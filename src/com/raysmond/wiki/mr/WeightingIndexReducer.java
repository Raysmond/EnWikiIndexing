package com.raysmond.wiki.mr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

import com.raysmond.wiki.writable.WeightingIndex;

public class WeightingIndexReducer extends
		TableReducer<Text, WeightingIndex, Text> {

	@Override
	public void reduce(Text key, Iterable<WeightingIndex> values,
			Context context) throws IOException, InterruptedException {
		ArrayList<WeightingIndex> list = new ArrayList<WeightingIndex>();
		for (WeightingIndex index : values) {
			list.add(new WeightingIndex(index));
		}
		WeightingIndex.numberOfDocumentsWithTerm = list.size();
		
		// Sort posting list by term weighting
		Collections.sort(list);

		StringBuilder str = new StringBuilder();
		str.append(list.size()).append(" ");

		Iterator<WeightingIndex> it = list.iterator();
		long lastId = it.next().getArticleId();
		str.append(lastId);

		Put put = new Put(Bytes.toBytes(key.toString()));

		int count = 0;
		while (it.hasNext()) {
			WeightingIndex index = it.next();
			str.append(" ").append(index.getArticleId()- lastId);

			if (str.length() >= 5000) {
				put.add(Bytes.toBytes("content"),
						Bytes.toBytes("index" + (count++)),
						Bytes.toBytes(str.toString()));
				str = new StringBuilder();
				lastId = 0L;
			} else
				lastId = index.getArticleId();
		}

		if (str.length() > 0)
			put.add(Bytes.toBytes("content"),
					Bytes.toBytes(new String("index" + count)),
					Bytes.toBytes(str.toString()));

		context.write(new Text(key), put);
	}
}
