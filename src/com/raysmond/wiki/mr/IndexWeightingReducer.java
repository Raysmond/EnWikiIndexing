package com.raysmond.wiki.mr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

import com.raysmond.wiki.util.CounterUtil;
import com.raysmond.wiki.writable.WordIndexWithoutPosition;

public class IndexWeightingReducer extends TableReducer<Text, WordIndexWithoutPosition, Text> {

	@Override
	public void reduce(Text key, Iterable<WordIndexWithoutPosition> values, Context context)
			throws IOException, InterruptedException {
		//System.out.println("start to reduce word: " + key.toString());
		
		ArrayList<WordIndexWithoutPosition> list = new ArrayList<WordIndexWithoutPosition>();
		for(WordIndexWithoutPosition index: values){
			list.add(new WordIndexWithoutPosition(index));
		}
		WordIndexWithoutPosition.numberOfDocumentsWithTerm = list.size();
		Collections.sort(list);
		
		StringBuilder str = new StringBuilder();
		int count = 0;
		for(WordIndexWithoutPosition index:list){
			//if(count++<10)
			//	str.append(index.getArticleId()).append(" ").append(index.getWeighting()).append(",");
			//else
				str.append(index.getArticleId() + " ");
		}
		String s = str.toString();
		if(s.length()>5000){
			System.out.println(key.toString()+" appears in " + list.size() + " pages.");
			s = s.substring(0,5000);
		}
		
		CounterUtil.countWord();
		CounterUtil.updateMaxWordAppearance(list.size());
			
		Put put = new Put(Bytes.toBytes(key.toString()));
		put.add(Bytes.toBytes("content"), Bytes.toBytes("weight"), Bytes.toBytes(str.toString()));
		context.write(new Text(key), put);
	}
}
