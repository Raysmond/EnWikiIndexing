package com.raysmond.wiki.mr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;

import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.Text;

import com.raysmond.wiki.util.CounterUtil;
import com.raysmond.wiki.writable.ArrayListWritable;
import com.raysmond.wiki.writable.WeightingIndex;
import com.raysmond.wiki.writable.WeightingIndexList;

/**
 * FileIndexingReducer
 * Reduce all indexes with term weighting, and store the result into HDFS
 * 
 * @author Raysmond
 *
 */
public class FileIndexingReducer extends
		TableReducer<Text, WeightingIndex, Text> {

	@Override
	public void reduce(Text key, Iterable<WeightingIndex> values,
			Context context) throws IOException, InterruptedException {
		WeightingIndexList<WeightingIndex> list = new WeightingIndexList<WeightingIndex>();
		Iterator<WeightingIndex> iter = values.iterator();
		while (iter.hasNext()) {
			WeightingIndex index = new WeightingIndex(iter.next());
			list.add(index);
		}

		WeightingIndex.numberOfDocumentsWithTerm = list.size();

		// Sort posting list by term weighting
		Collections.sort(list);

		// Compress article ids
		Long lastId = 0L;
		Iterator<WeightingIndex> it = list.iterator();
		while (it.hasNext()) {
			WeightingIndex index = it.next();
			Long _id = Long.parseLong(index.getArticleId());
			String id = String.valueOf(_id - lastId);
			lastId = _id;
			index.setArticleId(id);
		}

		context.write(key, list);

		CounterUtil.countWord();
		CounterUtil.updateMaxWordAppearance(list.size(), key.toString());
	}
}