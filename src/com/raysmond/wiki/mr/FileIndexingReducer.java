package com.raysmond.wiki.mr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobID;

import com.raysmond.wiki.util.CounterTag;
import com.raysmond.wiki.writable.ArrayListWritable;
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

		// Sort posting list by term weighting
		Collections.sort(list);
		
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