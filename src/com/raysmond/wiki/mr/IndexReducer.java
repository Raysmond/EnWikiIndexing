package com.raysmond.wiki.mr;

import java.io.IOException;
import java.util.Iterator;
import java.util.TreeMap;

import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.io.Text;

import com.raysmond.wiki.writable.IndexList;
import com.raysmond.wiki.writable.WordIndex;

/**
 * IndexReducer class
 * 
 * @author Raysmond
 * 
 */
public class IndexReducer extends MapReduceBase implements
		Reducer<Text, WordIndex, Text, IndexList> {

	private TreeMap<String, WordIndex> map;

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
				map.put(aid, new WordIndex(new String(aid), index.getTimes(),
						index.getPositions()));
			}
		}
		output.collect(key, new IndexList(map));
	}
}
