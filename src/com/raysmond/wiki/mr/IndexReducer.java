package com.raysmond.wiki.mr;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.io.Text;

import com.raysmond.wiki.writable.DocSumWritable;

/**
 * IndexReducer class
 * 
 * @author Raysmond
 * 
 */
public class IndexReducer extends MapReduceBase implements
		Reducer<Text, Text, Text, DocSumWritable> {

	private HashMap<String, Integer> map;

	private void addWord(String tag) {
		Integer val;

		if (map.get(tag) != null) {
			val = map.get(tag);
			map.remove(tag);
		} else {
			val = 0;
		}

		map.put(tag, val + 1);
	}

	@Override
	public void reduce(Text key, Iterator<Text> values,
			OutputCollector<Text, DocSumWritable> output, Reporter reporter)
			throws IOException {
		map = new HashMap<String, Integer>();

		while (values.hasNext()) {
			addWord(values.next().toString());
		}

		output.collect(key, new DocSumWritable(map));
	}
}
