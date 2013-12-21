package com.raysmond.wiki.util;

public class CounterUtil {
	private static long pageCount = 0;
	private static long wordCount = 0;
	private static int maxLengthOfWord = 0;
	private static long maxAppearanceInArticle = 0;
	
	public synchronized static void countPage(){
		CounterUtil.pageCount++;
	}
	
	public synchronized static void countWord(){
		CounterUtil.wordCount++;
	}

	public synchronized static void updateMaxWordLength(String word){
		int len = word.length();
		if(len>CounterUtil.maxLengthOfWord){
			CounterUtil.maxLengthOfWord = len;
		}
	}
	
	public synchronized static void updateMaxWordAppearance(long wordAppearance){
		if(wordAppearance>CounterUtil.maxAppearanceInArticle)
			CounterUtil.maxAppearanceInArticle = wordAppearance;
	}

	
	public static long getPageCount(){
		return CounterUtil.pageCount;
	}
	
	public static long getWordCount(){
		return CounterUtil.wordCount;
	}
	
	public static int getMaxWordLength(){
		return CounterUtil.maxLengthOfWord;
	}
	
	public static long getMaxAppearance(){
		return CounterUtil.maxAppearanceInArticle;
	}
}
