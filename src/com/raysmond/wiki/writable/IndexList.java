package com.raysmond.wiki.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.TreeMap;
import org.apache.hadoop.io.Writable;

/**
 * IndexList
 * An index list contains a list of WordIndex instance for one word. 
 * 
 * @author Raysmond
 *
 */
public class IndexList implements Writable{
	
	// Article id to WikiIndex, TreeMap sorts results by article id automatically
	private TreeMap<String,WordIndex> map = new TreeMap<String,WordIndex>();
	
	public IndexList(){
		
	}
	
	public IndexList(TreeMap<String,WordIndex> map){
		this.map = map;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		Iterator<String> keys = map.keySet().iterator();
		while(keys.hasNext()){
		     WordIndex output = new WordIndex();
		     output.readFields(in);
		     map.put(output.getArticleId(), output);
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		Iterator<String> keys = map.keySet().iterator();
		while(keys.hasNext()){
		     WordIndex output = map.get(keys.next());
		     output.write(out);
		}
	}
	
	@Override
	public String toString(){
		StringBuilder str = new StringBuilder();
		Iterator<String> keys = map.keySet().iterator();
		while(keys.hasNext()){
			str.append(map.get(keys.next()).toString()).append("\t");
		}
		return str.toString();
	}

}
