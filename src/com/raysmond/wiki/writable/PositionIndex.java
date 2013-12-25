package com.raysmond.wiki.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
 * WordIndex An word index contains an article ID and a list of positions where
 * the word appears. The word it self is stored outside of WordIndex.
 * 
 * @author Raysmond
 */
public class PositionIndex implements WritableComparable<PositionIndex> {
	// The unique id of the page
	private String articleId;

	// The positions in term of word offset where a word appears
	private ArrayListWritable<IntWritable> positions = new ArrayListWritable<IntWritable>();

	public PositionIndex() {

	}

	public PositionIndex(PositionIndex index) {
		this.articleId = new String(index.articleId);
		for (IntWritable val : index.getPositions()) {
			positions.add(new IntWritable(val.get()));
		}
	}

	public PositionIndex(String articleId) {
		this.articleId = articleId;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		articleId = Text.readString(in);
		positions = new ArrayListWritable<IntWritable>();
		positions.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		Text.writeString(out, articleId);
		positions.write(out);
	}

	@Override
	public String toString() {
		StringBuilder str = new StringBuilder();
		str.append(articleId).append(" ");
		str.append(positions.size()).append(" ");
		for (IntWritable val : positions) {
			str.append(val).append(" ");
		}
		return str.toString();
	}
	
	@Override
	public int compareTo(PositionIndex other) {
		return articleId.compareTo(other.articleId);
	}
	
	private int lastPos = 0;
	public boolean addPosition(int pos) {
		positions.add(new IntWritable(pos - lastPos));
		lastPos = pos;
		return true;
	}

	public String getArticleId() {
		return articleId;
	}

	public ArrayListWritable<IntWritable> getPositions() {
		return positions;
	}
}
