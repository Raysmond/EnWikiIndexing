package com.raysmond.wiki.mr;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.raysmond.wiki.util.WikiPageUtil;
import com.raysmond.wiki.writable.WeightingIndex;

/**
 * IndexMapper class. The map will receive a page in XML form, and resolve it
 * into words and inverted indexes. The word index in the page contains the
 * current page ID and all positions where it appears in term of word offset.
 * 
 * @author Raysmond, Junshi Guo
 */
public class WeightingIndexMapper extends
		Mapper<LongWritable, Text, Text, WeightingIndex> {

	// Word to index map
	private HashMap<String, WeightingIndex> result;
	
	public final static int MAX_WORD_LENGTH = 255;

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		long id = Long.parseLong(WikiPageUtil.parseXMLTag("id", value));
		String text = WikiPageUtil.getPlainText(WikiPageUtil.parseXMLTag( "title", value) + "\n" + WikiPageUtil.parseXMLText(value));
		
		result = new HashMap<String, WeightingIndex>();

		int pos = 0;
		String[] words = text.split("[\\s+|[\\p{Punct}]+]+");
		for (String word : words) {
			if (word.length() <= MAX_WORD_LENGTH) 
				addWord(id, word.toLowerCase(), pos++);
		}

		Iterator<String> it = result.keySet().iterator();
		while (it.hasNext()) {
			String word = it.next();
			context.write(new Text(word.toLowerCase()), result.get(word));
		}
		
	}

	/**
	 * Add a word in a page and update the indexes.
	 * 
	 * @param articleId
	 *            page ID
	 * @param word
	 *            a word in page
	 * @param position
	 *            word offset in the page
	 */
	public void addWord(long articleId, String word, Integer position) {
		WeightingIndex output = result.get(word);
		if (output != null) {
			output.addPosition(position);
		} else {
			output = new WeightingIndex(articleId);
			output.addPosition(position);
			result.put(word, output);
		}
	}

}
