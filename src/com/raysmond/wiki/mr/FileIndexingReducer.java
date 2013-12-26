package com.raysmond.wiki.mr;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;

import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.Text;

import com.raysmond.wiki.util.CounterTag;
import com.raysmond.wiki.writable.IndexList;
import com.raysmond.wiki.writable.WeightingIndex;

/**
 * FileIndexingReducer Reduce all indexes with term weighting, and store the
 * result into HDFS
 * 
 * @author Raysmond
 * 
 */
public class FileIndexingReducer extends
		TableReducer<Text, WeightingIndex, Text> {

	@Override
	public void reduce(Text key, Iterable<WeightingIndex> values,
			Context context) throws IOException, InterruptedException {
		IndexList<WeightingIndex> list = new IndexList<WeightingIndex>();
		Iterator<WeightingIndex> iter = values.iterator();
		while (iter.hasNext()) {
			WeightingIndex index = new WeightingIndex(iter.next());
			list.add(index);
		}
		
		// update total words counter
		context.getCounter(CounterTag.TOTAL_WORDS).increment(1);
		// update max word occurence
		long maxOccurence = context.getCounter(
				CounterTag.MAX_WORD_OCCURENCE_COUNT).getValue();
		if (list.size() > maxOccurence)
			context.getCounter(CounterTag.MAX_WORD_OCCURENCE_COUNT).setValue(
					list.size());

		WeightingIndex.pageCount = context.getConfiguration().getLong("total_pages",0);
		WeightingIndex.numberOfDocumentsWithTerm = list.size();

		final long totalPages = context.getConfiguration().getLong("total_pages",0);
		final long wordPages = list.size();
		if(totalPages == 0){
			// sort by id
			Collections.sort(list);
		}
		else{
			// sort by weight
			Collections.sort(list, new Comparator<WeightingIndex>() {
	            @Override
	            public int compare(WeightingIndex index1, WeightingIndex index2) {
	            		long v =  index1.getWeight(totalPages, wordPages) - index2.getWeight(totalPages, wordPages);
	            		if(v>0) return -1;
	            		if(v<0) return 1;
	            		return 0;
	            	}
				}
	        );
		}
		
		// Compress article ids
		long lastId = 0L;
		Iterator<WeightingIndex> it = list.iterator();
		while (it.hasNext()) {
			WeightingIndex index = it.next();
			long _id = index.getArticleId();
			index.setArticleId(_id - lastId);
			lastId = _id;
		}

		context.write(key, list);
	}
}