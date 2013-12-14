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

public class MapOutput implements Writable {
	private String articleId;
	private int times = 0;
	private HashSet<Integer> positions = new HashSet<Integer>();

	public MapOutput() {

	}
	
	public MapOutput(String articleId){
		this.articleId = articleId;
		this.times = 0;
	}

	public MapOutput(String articleId, int times, HashSet<Integer> positions) {
		this.articleId = articleId;
		this.times = times;
		this.positions = positions;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		articleId = Text.readString(in);
		times = in.readInt();
		positions = new HashSet<Integer>();
		for (int i = 0; i < times; i++) {
			positions.add(in.readInt());
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		Text.writeString(out, articleId);
		out.writeInt(times);
		Iterator<Integer> it = positions.iterator();
		while (it.hasNext()) {
			out.writeInt(it.next());
		}
	}

	@Override
	public String toString() {
		StringBuilder str = new StringBuilder();
		str.append("{");
		str.append(articleId).append(" ");
		str.append(times).append(" ");
		Iterator<Integer> it = positions.iterator();
		str.append("(");
		while (it.hasNext()) {
			str.append(it.next()).append(" ");
		}
		str.append(")}");
		return str.toString();
	}
	
	public void addPosition(Integer pos){
		if(this.positions.add(pos)){
			this.times++;
		}
	}
	
	public String getArticleId(){
		return this.articleId;
	}
	
	public int getTimes(){
		return this.times;
	}
	
	public HashSet<Integer> getPositions(){
		return this.positions;
	}
}
