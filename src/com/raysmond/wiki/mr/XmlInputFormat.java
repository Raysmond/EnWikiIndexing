/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.raysmond.wiki.mr;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.regionserver.metrics.SchemaMetrics;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

/**
 *  Reads records that are delimited by a specific begin/end tag.
 *  
 * @author Raysmond, Junshi Guo
 */
public class XmlInputFormat extends TextInputFormat {

	public static final String START_TAG_KEY = "xmlinput.start";
	public static final String END_TAG_KEY = "xmlinput.end";

	@Override
	public RecordReader<LongWritable, Text> createRecordReader(
			InputSplit split, TaskAttemptContext context) {
		return new XmlRecordReader();
	}

	/**
	 * XMLRecordReader class to read through a given xml document to output xml
	 * blocks as records as specified by the start tag and end tag
	 * 
	 */
	public static class XmlRecordReader extends
			RecordReader<LongWritable, Text> {
		private byte[] startTag;
		private byte[] endTag;
		private long start;
		private long end;
		private FSDataInputStream fsin;
		private final DataOutputBuffer buffer = new DataOutputBuffer();

		LongWritable currentKey = null;
		Text currentValue = null;

		@Override
		public void close() throws IOException {
			fsin.close();
		}

		@Override
		public LongWritable getCurrentKey() throws IOException,
				InterruptedException {
			return currentKey;
		}

		@Override
		public Text getCurrentValue() throws IOException, InterruptedException {
			return currentValue;
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			return (fsin.getPos() - start) / (float) (end - start);
		}

		@Override
		public void initialize(InputSplit split, TaskAttemptContext context)
				throws IOException, InterruptedException {
			SchemaMetrics.configureGlobally(context.getConfiguration());
			startTag = "<page>".getBytes("utf-8");
			endTag = "</page>".getBytes("utf-8");

			// open the file and seek to the start of the split
			start = ((FileSplit) split).getStart();
			end = start + split.getLength();
			FileSystem fs = FileSystem.get(context.getConfiguration());
			Path file = ((FileSplit) split).getPath();
			fsin = fs.open(file);
			fsin.seek(start);
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			currentKey = new LongWritable();
			currentValue = new Text();
			if (fsin.getPos() < end) {
				if (readUntilMatch(startTag, false)) {
					try {
						buffer.write(startTag);
						if (readUntilMatch(endTag, true)) {
							currentKey.set(fsin.getPos());
							currentValue.set(buffer.getData(), 0,
									buffer.getLength());
							return true;
						}
					} finally {
						buffer.reset();
					}
				}
			}
			return false;
		}
		
		private void initTag(){
			
		}

		private boolean readUntilMatch(byte[] match, boolean withinBlock)
				throws IOException {
			int i = 0;
			while (true) {
				int b = fsin.read();
				// end of file:
				if (b == -1)
					return false;
				// save to buffer:
				if (withinBlock)
					buffer.write(b);

				// check if we're matching:
				if (b == match[i]) {
					i++;
					if (i >= match.length)
						return true;
				} else
					i = 0;
				// see if we've passed the stop point:
				if (!withinBlock && i == 0 && fsin.getPos() >= end)
					return false;
			}
		}
	}
}