package com.raysmond.wiki.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;


/**
 * WordIndex
 * An word index contains an article ID and a list of positions where the word appears. 
 * The word it self is stored outside of WordIndex.
 * 
 * @author Raysmond
 */
public class PositionIndex implements WritableComparable {
	// The unique id of the page
	private String articleId;

	// The positions in term of word offset where a word appears
	// private HashSet<Integer> positions = new HashSet<Integer>();
	private ArrayList<Integer> positions = new ArrayList<Integer>();
	
	private int positionCount = 0;

	public PositionIndex() {

	}
	
	public PositionIndex(PositionIndex index){
		this.articleId = new String(index.articleId);
		this.positionCount = index.positionCount;
		for(Integer val: index.getPositions()){
			positions.add(val);
		}
	}

	public PositionIndex(String articleId) {
		this.articleId = articleId;
		positionCount = 0;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		articleId = Text.readString(in);
		positionCount = in.readInt();
		positions = new ArrayList<Integer>();
	    for(int i=0;i<positionCount;i++){
	    	positions.add(in.readInt());
	    }
	}

	@Override
	public void write(DataOutput out) throws IOException {
		Text.writeString(out, articleId);
		out.writeInt(positionCount);
		for(Integer val: positions){
			out.writeInt(val);
		}
	}

	@Override
	public String toString() {
		StringBuilder str = new StringBuilder();
		str.append(articleId).append(" ");
		//str.append(positionCount).append(" ");
		for(Integer val: positions){
			str.append(val).append(" ");
		}
		return str.toString();
	}

	
	public boolean addPosition(Integer pos) {
		 if(positions.add(pos)){
			 positionCount++;
			 return true;
		 }
		 return false;
	}

	public String getArticleId() {
		return this.articleId;
	}

	public ArrayList<Integer> getPositions() {
		return this.positions;
	}

	@Override
	public int compareTo(Object o) {
		return this.articleId.compareTo(((PositionIndex)o).articleId);
	}
}
