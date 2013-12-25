package com.raysmond.wiki.mr;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.raysmond.wiki.util.CounterUtil;
import com.raysmond.wiki.util.WikiPageUtil;
import com.raysmond.wiki.writable.PositionIndex;

/**
 * IndexMapper class.
 * The map will receive a page in XML form, and resolve it into words and inverted indexes.
 * The word index in the page contains the current page ID and all positions where it appears
 * in term of word offset.
 * 
 * @author Raysmond, Junshi Guo
 */
public class PositionIndexMapper extends Mapper<LongWritable,Text,Text,PositionIndex> {
	
	// Word to index map
	private HashMap<String,PositionIndex> result;

	public void map(LongWritable key, Text page, Context context)
			throws IOException, InterruptedException {
		String id = WikiPageUtil.parseXMLTag("id", page);
		String text = WikiPageUtil.getPlainText(WikiPageUtil.parseXMLTag(
				"title", page) + "\n" + WikiPageUtil.parseXMLText(page));
		
		int pos = 0;
		result = new HashMap<String,PositionIndex>();
		
		String[] words = text.split("[\\s+|[\\p{Punct}]+]+");
		for(String word: words){
			CounterUtil.updateMaxWordLength(word);
			this.addWord(id,word,pos++);
		}
		
		Iterator<String> it = result.keySet().iterator();
		while(it.hasNext()){
			String word = it.next();
			//context.write(new Text(word.toLowerCase()), new WordIndex(result.get(word)));
			context.write(new Text(word.toLowerCase()), result.get(word));
		}
	}
	
	public void addWord(String articleId,String word, Integer position){
		PositionIndex output = result.get(word);
		if(output!=null){
			output.addPosition(position);
		}
		else{
			output = new PositionIndex(articleId);
			output.addPosition(position);
			result.put(word, output);
		}
		
	}
	
}
