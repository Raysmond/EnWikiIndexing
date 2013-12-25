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
public class PositionIndexList implements Writable{
	
	// Article id to WikiIndex, TreeMap sorts results by article id automatically
	private TreeMap<String,PositionIndex> map = new TreeMap<String,PositionIndex>();
	
	public PositionIndexList(){
		
	}
	
	public PositionIndexList(TreeMap<String,PositionIndex> map){
		this.map = map;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		Iterator<String> keys = map.keySet().iterator();
		while(keys.hasNext()){
		     PositionIndex output = new PositionIndex();
		     output.readFields(in);
		     map.put(output.getArticleId(), output);
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		Iterator<String> keys = map.keySet().iterator();
		while(keys.hasNext()){
		     PositionIndex output = map.get(keys.next());
		     output.write(out);
		}
	}
	
	@Override
	public String toString(){
		StringBuilder str = new StringBuilder();
		Iterator<String> keys = map.keySet().iterator();
		while(keys.hasNext()){
			//str.append("{").append(map.get(keys.next()).toString()).append("}");
			str.append(map.get(keys.next()).toString()).append(" | ");
			//if(keys.hasNext())
			//	str.append(",");
		}
		return str.toString();
	}

}
