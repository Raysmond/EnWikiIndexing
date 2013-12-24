package com.raysmond.wiki.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigDecimal;
import java.text.DecimalFormat;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import com.raysmond.wiki.util.CounterUtil;

public class WordIndexWithoutPosition implements WritableComparable {
	// The unique id of the page
	private String articleId;
	private int positionCount = 0;
	
	private long weighting = 0;
	
	public static long numberOfDocumentsWithTerm = 0;

	public WordIndexWithoutPosition() {

	}
	
	public WordIndexWithoutPosition(WordIndexWithoutPosition index){
		this.articleId = new String(index.articleId);
		this.positionCount = index.positionCount;
	}

	public WordIndexWithoutPosition(String articleId) {
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
		return this.articleId + " " + this.positionCount;
	}

	
	public boolean addPosition(Integer pos) {
		 positionCount++;
		 return true;
	}

	public String getArticleId() {
		return this.articleId;
	}
	
	public int getPositionCount(){
		return this.positionCount;
	}
	
	public static double round(double value, int scale, 
            int roundingMode) {   
       BigDecimal bd = new BigDecimal(value);   
       bd = bd.setScale(scale, roundingMode);   
       double d = bd.doubleValue();   
       bd = null;   
       return d;   
    }   
	
	public double getWeighting(){
		if(numberOfDocumentsWithTerm!=0 && weighting == 0){
			double weight = positionCount * Math.log((double)(CounterUtil.getPageCount())/(double)(numberOfDocumentsWithTerm));
			weighting = (long)(weight * 1000000);
		}
		return weighting;
	}

	@Override
	public int compareTo(Object o) {
		double w = getWeighting() - (((WordIndexWithoutPosition)o).getWeighting());
		if(w>0)
			return -1;
		else if(w<0)
			return 1;
		else return 0;
	}
}
