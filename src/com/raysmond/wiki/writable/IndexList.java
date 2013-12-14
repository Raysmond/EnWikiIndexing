package com.raysmond.wiki.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.TreeMap;

import org.apache.hadoop.io.Writable;

public class IndexList implements Writable{
	
	// Article id to MapOutput, TreeMap sorts results by article id automatically
	private TreeMap<String,MapOutput> map = new TreeMap<String,MapOutput>();
	
	public IndexList(){
		
	}
	
	public IndexList(TreeMap<String,MapOutput> map){
		this.map = map;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		Iterator<String> keys = map.keySet().iterator();
		while(keys.hasNext()){
		     MapOutput output = new MapOutput();
		     output.readFields(in);
		     map.put(output.getArticleId(), output);
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		Iterator<String> keys = map.keySet().iterator();
		while(keys.hasNext()){
		     MapOutput output = map.get(keys.next());
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
