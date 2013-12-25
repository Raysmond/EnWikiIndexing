package com.raysmond.wiki.mr;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import com.raysmond.wiki.util.WikiPageUtil;

// TODO
public class IdTitleMapper extends Mapper<LongWritable, Text, Text, Text>{
	
	public final static int MAX_WORD_LENGTH = 255;
	
	public void map(LongWritable key, Text page, Context context)
			throws IOException, InterruptedException {
		String id = WikiPageUtil.parseXMLTag("id", page);
		String title = WikiPageUtil.parseXMLTag("title", page);
		if(title.length() <= MAX_WORD_LENGTH)
			context.write(new Text(id), new Text(title));
	}

}
