package com.raysmond.wiki.mr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;

import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.Text;

import com.raysmond.wiki.util.CounterUtil;
import com.raysmond.wiki.writable.WeightingIndex;

public class FileIndexingReducer extends
		TableReducer<Text, WeightingIndex, Text> {

	@Override
	public void reduce(Text key, Iterable<WeightingIndex> values,
			Context context) throws IOException, InterruptedException {
		ArrayList<WeightingIndex> list = new ArrayList<WeightingIndex>();
		for (WeightingIndex index : values) {
			list.add(new WeightingIndex(index));
		}
		CounterUtil.countWord();
		CounterUtil.updateMaxWordAppearance(list.size(), key.toString());
		WeightingIndex.numberOfDocumentsWithTerm = list.size();

		// Sort posting list by term weighting
		Collections.sort(list);

		StringBuilder str = new StringBuilder();
		str.append(list.size()).append(" ");

		Iterator<WeightingIndex> it = list.iterator();
		Long lastId = Long.parseLong(it.next().getArticleId());
		str.append(lastId);
		while (it.hasNext()) {
			WeightingIndex index = it.next();
			str.append(" ").append(Long.parseLong(index.getArticleId()) - lastId);
			lastId = Long.parseLong(index.getArticleId());
		}
		
		// TODO
		// context.write(key, value);
	}
}