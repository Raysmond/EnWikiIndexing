package com.raysmond.wiki.util;

import info.bliki.wiki.filter.PlainTextConverter;
import info.bliki.wiki.model.WikiModel;

import java.nio.charset.CharacterCodingException;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.io.Text;

/**
 * A common helper class for process wiki page XML
 * 
 * @author Raysmond
 */
public class WikiPageUtil {
	private static final Pattern REF = Pattern.compile("<ref>.*?</ref>");
	private static final Pattern LANG_LINKS = Pattern.compile("\\[\\[[a-z\\-]+:[^\\]]+\\]\\]");
	private static final Pattern DOUBLE_CURLY = Pattern.compile("\\{\\{.*?\\}\\}");
	private static final Pattern URL = Pattern.compile("http://[^ <]+"); 
	private static final Pattern HTML_TAG = Pattern.compile("<[^!][^>]*>");
	private static final Pattern HTML_COMMENT = Pattern.compile("<!--.*?-->", Pattern.DOTALL);
	
	public final static int MAX_WORD_LENGTH = 255;
	
	/**
	 * Parse article from XML
	 * 
	 * @param tag
	 * @param article
	 * @return
	 * @throws CharacterCodingException
	 */
	public static String parseXMLTag(String tag, Text article)
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
	public static String parseXMLText(Text article) throws CharacterCodingException {
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

	public static String getPlainText(String s) {
		WikiModel wikiModel = new WikiModel("", "");
		PlainTextConverter textConverter = new PlainTextConverter();

		// Bliki doesn't seem to properly handle inter-language links, so remove
		// manually.
		s = LANG_LINKS.matcher(s).replaceAll(" ");

		wikiModel.setUp();
		s = wikiModel.render(textConverter, s);
		wikiModel.tearDown();

		// The way the some entities are encoded, we have to unescape twice.
		s = StringEscapeUtils.unescapeHtml(StringEscapeUtils.unescapeHtml(s));

		s = REF.matcher(s).replaceAll(" ");
		s = HTML_COMMENT.matcher(s).replaceAll(" ");

		// Sometimes, URL bumps up against comments e.g., <!--
		// http://foo.com/-->
		// Therefore, we want to remove the comment first; otherwise the URL
		// pattern might eat up
		// the comment terminator.
		s = URL.matcher(s).replaceAll(" ");
		s = DOUBLE_CURLY.matcher(s).replaceAll(" ");
		s = HTML_TAG.matcher(s).replaceAll(" ");

		return s;
	}
}
