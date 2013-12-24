package com.raysmond.wiki.util;

public class CounterUtil {
	public static long pageCount = 0;
	public static long wordCount = 0;
	public static int maxLengthOfWord = 0;
	public static long maxAppearanceInArticle = 0;
	public static String maxOccurenceWord = "";
	public static String maxLengthWord = "";
	
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
			maxLengthWord = word;
		}
	}
	
	public synchronized static void updateMaxWordAppearance(long wordAppearance, String word){
		if(wordAppearance>CounterUtil.maxAppearanceInArticle){
			maxOccurenceWord = word;
			CounterUtil.maxAppearanceInArticle = wordAppearance;
		}
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
