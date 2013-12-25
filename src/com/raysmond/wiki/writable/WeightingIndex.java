package com.raysmond.wiki.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import com.raysmond.wiki.util.CounterUtil;

/**
 * WeightingIndex
 * An index class with page id, position count and weighting value 
 * 
 * @author Raysmond
 */
public class WeightingIndex implements WritableComparable<WeightingIndex> {
	// The unique id of the page
	private String articleId;
	private int positionCount = 0;
	private long weighting = 0;
	
	// the word occurs in how many pages
	public static long numberOfDocumentsWithTerm = 0;

	public WeightingIndex() {

	}
	
	public WeightingIndex(WeightingIndex index){
		this.articleId = new String(index.articleId);
		this.positionCount = index.positionCount;
	}

	public WeightingIndex(String articleId) {
		this.articleId = articleId;
		positionCount = 0;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		articleId = Text.readString(in);
		positionCount = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		Text.writeString(out, articleId);
		out.writeInt(positionCount);
	}

	@Override
	public String toString() {
		return this.articleId;
	}

	
	public boolean addPosition(Integer pos) {
		 positionCount++;
		 return true;
	}

	public void setArticleId(String id){
		this.articleId = id;
	}
	
	public String getArticleId() {
		return this.articleId;
	}
	
	public int getPositionCount(){
		return this.positionCount;
	}
	
	public double getWeighting(){
		if(numberOfDocumentsWithTerm!=0 && weighting == 0){
			double weight = positionCount * Math.log((double)(CounterUtil.getPageCount())/(double)(numberOfDocumentsWithTerm));
			weighting = (long)(weight * 1000000);
		}
		return weighting;
	}

	@Override
	public int compareTo(WeightingIndex o) {
		double w = getWeighting() - o.getWeighting();
		if(w>0)
			return -1;
		else if(w<0)
			return 1;
		else return 0;
	}
}
