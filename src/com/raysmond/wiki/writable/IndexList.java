package com.raysmond.wiki.writable;

import java.util.Iterator;

import org.apache.hadoop.io.Writable;

/**
 * IndexList base class
 * 
 * @author Raysmond
 *
 * @param <E>
 */
public class IndexList<E extends Writable> extends
		ArrayListWritable<E> {
	private static final long serialVersionUID = -2838690040583088632L;
	
	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append(this.size()).append(" ");
		Iterator<E> iter = this.iterator();
		while (iter.hasNext()) {
			sb.append(iter.next()).append(" ");
		}
		return sb.toString();
	}

}
