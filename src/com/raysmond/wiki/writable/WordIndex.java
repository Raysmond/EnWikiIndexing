package com.raysmond.wiki.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class WordIndex implements Writable {
	// The unique id of the page
	private String articleId;

	// The positions in term of word offset where a word appears
	private HashSet<Integer> positions = new HashSet<Integer>();

	public WordIndex() {

	}

	public WordIndex(String articleId) {
		this.articleId = articleId;
	}

	public WordIndex(String articleId, HashSet<Integer> positions) {
		this.articleId = articleId;
		this.positions = positions;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		articleId = Text.readString(in);
		Iterator<Integer> it = positions.iterator();
		while (it.hasNext()) {
			positions.add(in.readInt());
		}		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		Text.writeString(out, articleId);
		Iterator<Integer> it = positions.iterator();
		while (it.hasNext()) {
			out.writeInt(it.next());
		}
	}

	@Override
	public String toString() {
		StringBuilder str = new StringBuilder();
		str.append(articleId).append(" ");
		Iterator<Integer> it = positions.iterator();
		while (it.hasNext()) {
			str.append(it.next()).append(" ");
		}
		return str.toString();
	}

	public boolean addPosition(Integer pos) {
		return positions.add(pos);
	}

	public String getArticleId() {
		return this.articleId;
	}

	public HashSet<Integer> getPositions() {
		return this.positions;
	}
}
