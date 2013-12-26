package com.raysmond.wiki.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
 * WeightingIndex An index class with page id, position count and weighting
 * value
 * 
 * @author Raysmond
 */
public class WeightingIndex implements WritableComparable<WeightingIndex> {
	// The unique id of the page
	private Long articleId;
	private int positionCount = 0;
	private long weighting = 0;

	// the word occurs in how many pages
	public static long numberOfDocumentsWithTerm = 0;
	public static long pageCount = 0;

	public WeightingIndex() {

	}

	public WeightingIndex(WeightingIndex index) {
		this.articleId = index.articleId;
		this.positionCount = index.positionCount;
	}

	public WeightingIndex(long articleId) {
		this.articleId = articleId;
		positionCount = 0;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		articleId = in.readLong();
		positionCount = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(articleId);
		out.writeInt(positionCount);
	}

	@Override
	public String toString() {
		return String.valueOf(articleId);
	}

	public boolean addPosition(Integer pos) {
		positionCount++;
		return true;
	}

	public void setArticleId(long id) {
		this.articleId = id;
	}

	public long getArticleId() {
		return articleId;
	}

	public int getPositionCount() {
		return this.positionCount;
	}

//	public long getWeighting() {
//		if (numberOfDocumentsWithTerm != 0 && pageCount != 0 && weighting == 0) {
//			double weight = positionCount
//					* Math.log((double) (pageCount)
//							/ (double) (numberOfDocumentsWithTerm));
//			weighting = (long) (weight * 1000000);
//		}
//		return weighting;
//	}

	public long getWeight(long totalPage, long termPages) {
		double w = positionCount * Math.log((double) (totalPage) / (double) (termPages));
		return (long) (w * 1000000);
	}

	public boolean equals(WeightingIndex obj) {
		return articleId == obj.articleId;
	}

	public int hashCode() {
		return articleId.hashCode();
	}

	@Override
	public int compareTo(WeightingIndex o) {
		long v = this.articleId - o.articleId;
		if (v > 0)
			return 1;
		if (v < 0)
			return -1;
		return 0;
	}
}
