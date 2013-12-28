package com.junshiguo.wiki.lucene;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
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
	public int topNo = 100;
	public String searchString = null;
	public ArrayList<Integer> postingList = null;
	public int totalPage = 0;
	public int pageSize = 10;

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
				JSONObject result = wikiSearch.doSearch();
				System.out.println(result.toString());
				System.out.print("> ");
			}
		} else {
			wikiSearch.setWIWIndexPath(args[1]);
			wikiSearch.setITIndexPath(args[2]);
			wikiSearch.setSearchString(args[0]);
			if (args.length > 3 && args[3] != null) {
				try {
					wikiSearch.setTopNo(Integer.parseInt(args[3]));
				} catch (Exception e) {
					System.out.println("Return number setting failed...");
				}
			}
			wikiSearch.doSearch();
		}
	}

	// @SuppressWarnings("deprecation")
	// public JSONObject doSearch(int topNo) throws IOException, ParseException
	// {
	// // query
	// Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_46);
	// QueryParser parser = new QueryParser(Version.LUCENE_46, "key", analyzer);
	// Query query = parser.parse(searchString);
	// // search in wordIndexWeight
	// IndexSearcher searcher = new IndexSearcher(IndexReader.open(FSDirectory
	// .open(new File(WIWIndexPath))));
	// TopScoreDocCollector collector = TopScoreDocCollector.create(topNo,
	// true);
	// searcher.search(query, collector);
	// ScoreDoc[] words = collector.topDocs().scoreDocs;
	//
	// JSONObject result = new JSONObject();
	// JSONArray results = new JSONArray();
	//
	// result.put("resultCount", words.length);
	//
	// // scan all matched words
	// for (int i = 0; i < words.length; i++) {
	// int docId = words[i].doc;
	// Document doc = searcher.doc(docId);
	// String index = doc.get("value").trim();
	// String[] indexes = index.split("\\s+", topNo + 2);
	// int pageCount = indexes.length
	// - (indexes.length == (topNo + 2) ? 2 : 1);
	//
	// // single result item
	// JSONObject item = new JSONObject();
	// item.put("word", doc.get("key"));
	// item.put("pageCount", pageCount);
	// JSONArray titleArray = new JSONArray();
	//
	// int pid = 0;
	// for (int j = 0; j < pageCount; j++) {
	// pid += Integer.parseInt(indexes[j + 1]);
	// String title = searchTitle(String.valueOf(pid));
	//
	// JSONObject page = new JSONObject();
	// page.put("index", j + 1);
	// page.put("pageId", pid);
	// page.put("title", title);
	// titleArray.put(j, page);
	// }
	//
	// item.put("pages", titleArray);
	// results.put(item);
	// }
	// result.put("result", results);
	// return result;
	// }

	/**
	 * set postingList and totalPage and return content of the first page in
	 * json format (equals doSearch(0)) page: wiki page; pager: sth related to
	 * pager at the front end
	 * 
	 * @return content of the first page in json format
	 * @throws ParseException
	 * @throws IOException
	 */
	public JSONObject doSearch() throws ParseException, IOException {
		this.setPostingList(searchString, WIWIndexPath, topNo);

		JSONObject result = new JSONObject();
		result.put("word", searchString);
		result.put("totalPager", Math.ceil(1.0 * totalPage / pageSize));
		result.put("pageNumber", 1);
		JSONArray titleArray = new JSONArray();

		int thisPageSize = (pageSize < totalPage) ? pageSize : totalPage;
		for (int i = 0; i < thisPageSize; i++) {
			String title = searchTitle(postingList.get(i));

			JSONObject page = new JSONObject();
			page.put("index", i + 1);
			page.put("pageId", postingList.get(i));
			page.put("title", title);
			titleArray.put(i, page);
		}
		result.put("pages", titleArray);
		return result;
	}

	/**
	 * @param pagerId
	 * @return searchResults in page No.pagerId
	 * @throws ParseException
	 * @throws IOException
	 */
	public JSONObject doSearch(int pagerId) throws ParseException, IOException {
		JSONObject result = new JSONObject();
		result.put("word", searchString);
		result.put("totalPager", Math.ceil(1.0 * totalPage / pageSize));
		result.put("pageNumber", pagerId + 1);
		JSONArray titleArray = new JSONArray();

		int thisPageSize = (pageSize * (pagerId + 1) < totalPage) ? pageSize
				: totalPage - pageSize * pagerId;
		for (int i = 0; i < thisPageSize; i++) {
			String title = searchTitle(postingList.get(i + pagerId * pageSize));
			JSONObject page = new JSONObject();
			page.put("index", i + 1);
			page.put("pageId", postingList.get(i + pagerId * pageSize));
			page.put("title", title);
			titleArray.put(i, page);
		}
		result.put("pages", titleArray);
		return result;
	}

	/**
	 * set postingList and totalPage if there exists an exact match of
	 * searchString, the pageId list of the exact match will be returned else
	 * the pageId list of top topNo matched pages will be returned
	 * 
	 * @param searchStr
	 * @param indexPath
	 * @param topNo
	 *            default 100
	 * @throws ParseException
	 * @throws IOException
	 */
	public void setPostingList(String searchStr, String indexPath, int topNo)
			throws ParseException, IOException {
		// query
		Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_46);
		QueryParser parser = new QueryParser(Version.LUCENE_46, "key", analyzer);
		Query query = parser.parse(searchStr);
		// search in wordIndexWeight
		DirectoryReader ireader = DirectoryReader.open(FSDirectory
				.open(new File(WIWIndexPath)));
		IndexSearcher searcher = new IndexSearcher(ireader);
		TopScoreDocCollector collector = TopScoreDocCollector.create(topNo,
				true);
		searcher.search(query, collector);
		ScoreDoc[] words = collector.topDocs().scoreDocs;

		boolean isExactMatch = false;
		int returnPageCount = 0;
		postingList = new ArrayList<Integer>();
		// scan all matched words
		for (int i = 0; i < words.length; i++) {
			int docId = words[i].doc;
			Document doc = searcher.doc(docId);
			String index = doc.get("value").trim();
			int pageCount;
			String[] indexes = null;
			if (doc.get("key") == searchString) { // exact match: return all
													// pages in result1
				isExactMatch = true;
				indexes = index.split("\\s+");
				pageCount = indexes.length - 1;
				returnPageCount += pageCount;
			} else {
				indexes = index.split("\\s+", topNo + 2 - returnPageCount);
				pageCount = indexes.length
						- (indexes.length == (topNo + 2 - returnPageCount) ? 2
								: 1);
				returnPageCount += pageCount;
			}

			int pid = 0;
			for (int j = 0; j < pageCount; j++) {
				pid += Integer.parseInt(indexes[j + 1]);
				postingList.add(pid);
			}

			if (isExactMatch)
				break; // exact match: return pages in result1
			if (returnPageCount == topNo)
				break; // not exact match: return topNo pages
		}
		// postingList.add(0, returnPageCount);
		totalPage = returnPageCount;
	}

	/**
	 * Search page title by page id
	 * 
	 * @param articleId
	 * @return String|null page title
	 * @throws ParseException
	 * @throws IOException
	 */
	@SuppressWarnings("deprecation")
	public String searchTitle(int articleId) throws ParseException, IOException {
		// query
		Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_46);
		QueryParser parser = new QueryParser(Version.LUCENE_46, "key", analyzer);
		Query query = parser.parse("" + articleId);

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

	public int getTopNo() {
		return topNo;
	}

	public void setTopNo(int topNo) {
		this.topNo = topNo;
	}

	public ArrayList<Integer> getPostingList() {
		return postingList;
	}

	public void setPostingList(ArrayList<Integer> postingList) {
		this.postingList = postingList;
	}

}
