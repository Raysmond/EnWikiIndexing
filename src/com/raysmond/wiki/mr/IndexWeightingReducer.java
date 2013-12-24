package com.raysmond.wiki.mr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

import com.raysmond.wiki.util.CounterUtil;
import com.raysmond.wiki.writable.WordIndexWithoutPosition;

public class IndexWeightingReducer extends
		TableReducer<Text, WordIndexWithoutPosition, Text> {

	@Override
	public void reduce(Text key, Iterable<WordIndexWithoutPosition> values,
			Context context) throws IOException, InterruptedException {
		ArrayList<WordIndexWithoutPosition> list = new ArrayList<WordIndexWithoutPosition>();
		for (WordIndexWithoutPosition index : values) {
			list.add(new WordIndexWithoutPosition(index));
		}
		CounterUtil.countWord();
		CounterUtil.updateMaxWordAppearance(list.size(), key.toString());
		WordIndexWithoutPosition.numberOfDocumentsWithTerm = list.size();
		
		// Sort posting list by term weighting
		Collections.sort(list);

		StringBuilder str = new StringBuilder();
		str.append(list.size()).append(" ");

		Iterator<WordIndexWithoutPosition> it = list.iterator();
		Long lastId = Long.parseLong(it.next().getArticleId());
		str.append(lastId);

		Put put = new Put(Bytes.toBytes(key.toString()));

		int count = 0;
		while (it.hasNext()) {
			WordIndexWithoutPosition index = it.next();
			str.append(" ").append(
					Long.parseLong(index.getArticleId()) - lastId);

			if (str.length() >= 5000) {
				put.add(Bytes.toBytes("content"),
						Bytes.toBytes("index" + (count++)),
						Bytes.toBytes(str.toString()));
				str = new StringBuilder();
				lastId = 0L;
			} else
				lastId = Long.parseLong(index.getArticleId());
		}

		if (str.length() > 0)
			put.add(Bytes.toBytes("content"),
					Bytes.toBytes(new String("index" + count)),
					Bytes.toBytes(str.toString()));

		context.write(new Text(key), put);
	}
}
