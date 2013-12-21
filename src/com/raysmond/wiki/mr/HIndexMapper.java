package com.raysmond.wiki.mr;
import info.bliki.wiki.filter.PlainTextConverter;
import info.bliki.wiki.model.WikiModel;

import java.io.IOException;
import java.nio.charset.CharacterCodingException;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.raysmond.wiki.util.StringUtils;
import com.raysmond.wiki.writable.WordIndex;

public class HIndexMapper extends Mapper<LongWritable,Text,Text,WordIndex> {
	private HashMap<String,WordIndex> result;

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String id = this.parseXMLTag("id", value);
		String content = this.parseXMLText(value);
		String plainStr = this.cleanText(content);
		
		String[] words = plainStr.split("\\s+");
		int pos = 0;
		result = new HashMap<String,WordIndex>();
		for(String word: words){
			//word = word.replaceAll("[^\\w]", "");
			//output.collect(new Text(word.toLowerCase()), new Text(id));
			this.addWord(id,word,pos++);
		}
		Iterator<String> it = result.keySet().iterator();
		while(it.hasNext()){
			String word = it.next();
			context.write(new Text(word.toLowerCase()), result.get(word));
		}
	}
	
	public void addWord(String articleId,String word, Integer position){
		WordIndex output = result.get(word);
		if(output!=null){
			output.addPosition(position);
			result.remove(word);
		}
		else{
			output = new WordIndex(articleId);
			output.addPosition(position);
		}
		result.put(word, output);
	}
	
	/**
	 * Parse article XML
	 * 
	 * @param tag
	 * @param article
	 * @return
	 * @throws CharacterCodingException
	 */
	private String parseXMLTag(String tag, Text article)
			throws CharacterCodingException {
		int start = article.find("<" + tag + ">");
		int end = article.find("</" + tag + ">");

		if (start == -1 || end == -1) {
			return "";
		} else {
			start += tag.length() + 2;
			return Text.decode(article.getBytes(), start, end - start);
		}
	}

	/**
	 * Parse article content
	 * 
	 * @param article
	 * @return
	 * @throws CharacterCodingException
	 */
	private String parseXMLText(Text article) throws CharacterCodingException {
		String tag = "<text xml:space=\"preserve\">";
		int start = article.find("<text xml:space=\"preserve\">");
		int end = article.find("</text>");

		if (start == -1 || end == -1) {
			return "";
		} else {
			start += tag.length();
			return Text.decode(article.getBytes(), start, end - start);
		}
	}
	

	/**
	 * Get plain text from XML text
	 * @param text
	 * @return
	 */
	private String cleanText(String text) {
//		text = EntityDecoder.entityToHtml(text);
		text = StringUtils.unescapeXML(text);
		
		text = text.replaceAll("[=]+[A-Za-z+\\s-]+[=]+", " ")
				.replaceAll("\\{\\{[A-Za-z0-9+\\s-]+\\}\\}", " ")
				.replaceAll("(?m)<ref>.+</ref>", " ")
				.replaceAll("(?m)<ref name=\"[A-Za-z0-9\\s-]+\">.+</ref>", " ")
				.replaceAll("<ref>", " <ref>");
		
		// Convert to plain text
		WikiModel wikiModel = new WikiModel("${image}", "${title}");

		// Remove text inside {{ }}
		String plainStr = wikiModel.render(new PlainTextConverter(), text)
				.replaceAll("\\{\\{[A-Za-z+\\s-]+\\}\\}", " ");

		return plainStr;
	}
	
}
