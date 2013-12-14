package com.raysmond.wiki.mr;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.TreeMap;

import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.io.Text;

import com.raysmond.wiki.writable.DocSumWritable;
import com.raysmond.wiki.writable.IndexList;
import com.raysmond.wiki.writable.MapOutput;

/**
 * IndexReducer class
 * 
 * @author Raysmond
 * 
 */
public class IndexReducer extends MapReduceBase implements
		Reducer<Text, MapOutput, Text, IndexList> {

	private TreeMap<String,MapOutput> map;

	private void addIndex(String articleId, MapOutput index) {
//		MapOutput val;
//		val = map.get(articleId);
		if (map.get(articleId) != null) {
			// not allowed 
			return;
		} else {
			System.out.println("put("+articleId+":"+index.toString()+")");
			map.put(articleId, index);
		}
	}

	@Override
	public void reduce(Text key, Iterator<MapOutput> values,
			OutputCollector<Text, IndexList> output, Reporter reporter)
			throws IOException {
		map = new TreeMap<String,MapOutput>();
		System.out.println(key+":");
		while(values.hasNext()){
			MapOutput index = values.next();
			this.addIndex(index.getArticleId(), index);
		}
		Iterator<String> keys = map.keySet().iterator();
		while(keys.hasNext()){
			String articleId = keys.next();
			System.out.print(articleId+":");
		    System.out.print(map.get(articleId).toString());
		}
		System.out.println();
		output.collect(key, new IndexList(map));
	}
}
