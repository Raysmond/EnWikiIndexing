package com.junshiguo.wiki.lucene;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

public class ExtraIdIndexBuilder extends IndexBuilder {

	@SuppressWarnings("deprecation")
	@Override
	public void build() throws IOException {
		IndexWriterConfig IWConf = new IndexWriterConfig(Version.LUCENE_46,
				new StandardAnalyzer(Version.LUCENE_46));
		IndexWriter indexWriter = new IndexWriter(FSDirectory.open(new File(
				outputPath)), IWConf);

		BufferedReader reader = new BufferedReader(new FileReader(inputFile));

		String row = null;

		int count = 0;
		while ((row = reader.readLine()) != null) {
			row = row.trim();
			String[] values = row.split("\\s+", 3);
			org.apache.lucene.document.Document document = new org.apache.lucene.document.Document();

			document.add(new Field("key", values[0]+"-"+values[1], Field.Store.YES,
					Field.Index.ANALYZED));
			document.add(new Field("value", values[1].trim(), Field.Store.YES, Field.Index.NOT_ANALYZED));
			indexWriter.addDocument(document);
		}
		indexWriter.close();
		reader.close();
	}

}
