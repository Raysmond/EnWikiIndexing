package com.raysmond.wiki.mr;

import info.bliki.wiki.filter.PlainTextConverter;
import info.bliki.wiki.model.WikiModel;

import java.io.IOException;
import java.nio.charset.CharacterCodingException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import com.raysmond.wiki.util.StringUtils;
import com.raysmond.wiki.writable.MapOutput;

/**
 * IndexMapper class
 * 
 * @author Raysmond
 * 
 */
public class IndexMapper extends MapReduceBase implements
		Mapper<LongWritable, Text, Text, MapOutput> {

	private HashMap<String,MapOutput> result;
	/**
	 * Map method
	 * 
	 * @param value
	 *            Text all XML content of a single page within <page> </page>
	 * @param output
	 *            OutputCollector<Text,Text> the first text is a word and the
	 *            second is the title (id maybe better) of the article
	 */
	@Override
	public void map(LongWritable key, Text value,
			OutputCollector<Text, MapOutput> output, Reporter reporter)
			throws IOException {

		String id = this.parseXMLTag("id", value);
		//String title = this.parseXMLTag("title", value);
		String content = this.parseXMLText(value);

		String plainStr = this.cleanText(content);

//		StringTokenizer tokenizer = new StringTokenizer(plainStr);
//		while (tokenizer.hasMoreTokens()) {
//			String token = tokenizer.nextToken();
//
//			output.collect(new Text(token.toLowerCase()), new Text(title));
//		}
		
		String[] words = plainStr.split("\\s+");
		int pos = 0;
		result = new HashMap<String,MapOutput>();
		for(String word: words){
			//word = word.replaceAll("[^\\w]", "");
			//output.collect(new Text(word.toLowerCase()), new Text(id));
			this.addWord(id,word,pos++);
		}
		Iterator<String> it = result.keySet().iterator();
		while(it.hasNext()){
			String word = it.next();
			System.out.println(word+":");
			System.out.println(result.get(word));
			System.out.println();
			output.collect(new Text(word.toLowerCase()), result.get(word));
		}
		
	}
	
	public void addWord(String articleId,String word, Integer position){
		MapOutput output = result.get(word);
		if(output!=null){
			output.addPosition(position);
			result.remove(word);
		}
		else{
			output = new MapOutput(articleId);
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
