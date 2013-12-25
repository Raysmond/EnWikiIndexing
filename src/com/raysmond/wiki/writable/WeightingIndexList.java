package com.raysmond.wiki.writable;

import java.util.Iterator;

import org.apache.hadoop.io.Writable;

public class WeightingIndexList<WeightingIndex extends Writable> extends
		ArrayListWritable<WeightingIndex> {
	private static final long serialVersionUID = -2838690040583088632L;
	
	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append(this.size()).append(" ");
		Iterator<WeightingIndex> iter = this.iterator();
		while (iter.hasNext()) {
			sb.append(iter.next()).append(" ");
		}
		return sb.toString();
	}

}
