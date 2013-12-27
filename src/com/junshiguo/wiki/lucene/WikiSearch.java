package com.junshiguo.wiki.lucene;

import java.io.File;
import java.io.IOException;
import java.util.Scanner;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;
import org.json.JSONArray;
import org.json.JSONObject;

/*
 * wordIndexWeight
 * idTitle
 */
public class WikiSearch {
	public String WIWIndexPath = null; // wordIndexWeight index path
	public String ITIndexPath = null; // idTitle index path
	public int retNo = 10;
	public String searchString = null;

	public static void main(String[] args) throws IOException, ParseException {
		WikiSearch wikiSearch = new WikiSearch();
		if (args.length < 3) {
			System.out
					.println("Please set wordIndexWeight and idTitle index path");
			Scanner in = new Scanner(System.in);
			System.out.print("Index path: ");
			wikiSearch.setWIWIndexPath(in.next());
			System.out.print("Title path: ");
			wikiSearch.setITIndexPath(in.next());
			String keyword = null;
			System.out.print("Type some key words: \n>");
			while ((keyword = in.nextLine()) != null) {
				if (keyword.equals(""))
					continue;
				wikiSearch.setSearchString(keyword.toLowerCase());
				JSONObject result = wikiSearch.doSearch(10);
				System.out.println(result.toString());
				System.out.print("> ");
			}
		} else {
			wikiSearch.setWIWIndexPath(args[1]);
			wikiSearch.setITIndexPath(args[2]);
			wikiSearch.setSearchString(args[0]);
			if (args.length > 3 && args[3] != null) {
				try {
					wikiSearch.setRetNo(Integer.parseInt(args[2]));
				} catch (Exception e) {
					System.out.println("Return number setting failed...");
				}
			}
			wikiSearch.doSearch(100);
		}
	}

	@SuppressWarnings("deprecation")
	public JSONObject doSearch(int topNo) throws IOException, ParseException {
		// query
		Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_46);
		QueryParser parser = new QueryParser(Version.LUCENE_46, "key", analyzer);
		Query query = parser.parse(searchString);
		// search in wordIndexWeight
		IndexSearcher searcher = new IndexSearcher(IndexReader.open(FSDirectory
				.open(new File(WIWIndexPath))));
		TopScoreDocCollector collector = TopScoreDocCollector.create(topNo,
				true);
		searcher.search(query, collector);
		ScoreDoc[] words = collector.topDocs().scoreDocs;

		JSONObject result = new JSONObject();
		JSONArray results = new JSONArray();

		result.put("resultCount", words.length);

		// scan all matched words
		for (int i = 0; i < words.length; i++) {
			int docId = words[i].doc;
			Document doc = searcher.doc(docId);
			String index = doc.get("value").trim();
			String[] indexes = index.split("\\s+", topNo + 2);
			int pageCount = indexes.length
					- (indexes.length == (topNo + 2) ? 2 : 1);
			
			// single result item
			JSONObject item = new JSONObject();
			item.put("word", doc.get("key"));
			item.put("pageCount", pageCount);
			JSONArray titleArray = new JSONArray();

			int pid = 0;
			for (int j = 0; j < pageCount; j++) {
				pid += Integer.parseInt(indexes[j + 1]);
				String title = searchTitle(String.valueOf(pid));
				
				JSONObject page = new JSONObject();
				page.put("index", j+1);
				page.put("pageId", pid);
				page.put("title", title);
				titleArray.put(j, page);
			}

			item.put("pages", titleArray);
			results.put(item);
		}
		result.put("result", results);
		return result;
	}

	/**
	 * Search page title by page id
	 * @param articleId
	 * @return String|null page title
	 * @throws ParseException
	 * @throws IOException
	 */
	@SuppressWarnings("deprecation")
	public String searchTitle(String articleId) throws ParseException,
			IOException {
		// query
		Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_46);
		QueryParser parser = new QueryParser(Version.LUCENE_46, "key", analyzer);
		Query query = parser.parse(articleId);

		// search in ITIndexPath
		FSDirectory dir = FSDirectory.open(new File(ITIndexPath));
		IndexReader reader = IndexReader.open(dir);
		IndexSearcher searcher = new IndexSearcher(reader);
		TopScoreDocCollector collector = TopScoreDocCollector.create(1, true);

		// do search
		searcher.search(query, collector);
		ScoreDoc[] hits = collector.topDocs().scoreDocs;

		String title = null;
		if (hits.length > 0) {
			int docId = hits[0].doc;
			Document doc = searcher.doc(docId);
			title = doc.get("value");
		}
		dir.close();
		reader.close();
		return title;
	}

	public String getSearchString() {
		return searchString;
	}

	public void setSearchString(String searchString) {
		this.searchString = searchString;
	}

	public String getWIWIndexPath() {
		return WIWIndexPath;
	}

	public void setWIWIndexPath(String wIWIndexPath) {
		WIWIndexPath = wIWIndexPath;
	}

	public String getITIndexPath() {
		return ITIndexPath;
	}

	public void setITIndexPath(String iTIndexPath) {
		ITIndexPath = iTIndexPath;
	}

	public int getRetNo() {
		return retNo;
	}

	public void setRetNo(int retNo) {
		this.retNo = retNo;
	}

}
