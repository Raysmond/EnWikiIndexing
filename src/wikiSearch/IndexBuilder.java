package wikiSearch;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;
/*
 * wordIndexWeight
 * idTitle
 */
public class IndexBuilder {
	public File inputFile = null;
	public String inputPath = null; //args[0]
	public String outputPath = null; //args[1]

	public static void main(String[] args) throws IOException {
		if(args.length <2){
			System.out.println("Please set input and output path");
			System.exit(1);
		}
		IndexBuilder indexBuilder = new IndexBuilder();
		indexBuilder.setInputPath(args[0]);
		indexBuilder.setOutputPath(args[1]);
		File[] files = new File(indexBuilder.getInputPath()).listFiles();
		for(File file: files){
			if(file.isDirectory() == false){
				indexBuilder.setInputFile(file);
				indexBuilder.build();
			}
		}
	}
	
	@SuppressWarnings("deprecation")
	public void build() throws IOException {
		IndexWriterConfig IWConf = new IndexWriterConfig(Version.LUCENE_46,
				new StandardAnalyzer(Version.LUCENE_46));
		IndexWriter indexWriter = new IndexWriter(FSDirectory.open(new File(
				outputPath)), IWConf);

		BufferedReader reader = new BufferedReader(new FileReader(inputFile));

		String row = null;

		while ((row = reader.readLine()) != null) {
			row = row.trim();
			String Arow[] = row.split("\\s+");
			org.apache.lucene.document.Document document = new org.apache.lucene.document.Document();

			document.add(new Field("key", Arow[0], Field.Store.YES,	Field.Index.ANALYZED));
			document.add(new Field("value",	row.substring(Arow[0].length()).trim(), Field.Store.YES,Field.Index.NOT_ANALYZED));

			indexWriter.addDocument(document);
		}
		indexWriter.close();
		reader.close();
	}
	
	public String getInputPath() {
		return inputPath;
	}

	public void setInputPath(String inputPath) {
		this.inputPath = inputPath;
	}

	public File getInputFile() {
		return inputFile;
	}

	public void setInputFile(File inputFile) {
		this.inputFile = inputFile;
	}

	public String getOutputPath() {
		return outputPath;
	}

	public void setOutputPath(String outputPath) {
		this.outputPath = outputPath;
	}

}
