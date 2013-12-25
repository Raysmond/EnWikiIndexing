package com.raysmond.wiki.mr;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.raysmond.wiki.util.CounterUtil;
import com.raysmond.wiki.util.WikiPageUtil;
import com.raysmond.wiki.writable.WeightingIndex;

/**
 * FileIndexingMapper
 * 
 * @author Raysmond
 * 
 */
public class FileIndexingMapper extends
		Mapper<LongWritable, Text, Text, WeightingIndex> {

	private HashMap<String, WeightingIndex> result;

	public void map(LongWritable key, Text page, Context context)
			throws IOException, InterruptedException {
		long id = Long.parseLong(WikiPageUtil.parseXMLTag("id", page));
		String text = WikiPageUtil.getPlainText(WikiPageUtil.parseXMLTag(
				"title", page) + "\n" + WikiPageUtil.parseXMLText(page));
		result = new HashMap<String, WeightingIndex>();

		int pos = 0;
		String[] words = text.split("[\\s+|[\\p{Punct}]+]+");
		for (String word : words) {
			if (word.length() <= WikiPageUtil.MAX_WORD_LENGTH)
				addWord(id, word, pos++);
		}

		Iterator<String> it = result.keySet().iterator();
		while (it.hasNext()) {
			String word = it.next();
			context.write(new Text(word.toLowerCase()), result.get(word));
		}

//		Configuration conf = context.getConfiguration();
//		conf.setLong("page_count",conf.getLong("page_count", 0) + 1);
	}

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
