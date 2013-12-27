EnWikiIndexing
==============================

A simple program to create inverted indexes for English Wikipedia dump using Hadoop. By the way, this's my final project for Distributed System course 2013 in Fudan University.

## Overview
The project can build inverted indexes for English Wikipedia XML dump file. The following inverted index types are supported.
* Normal indexes: TF + DF
* Indexes with term weighting: TF + IDF
* Positional indexes: TF + DF + positions

Inverted indexes are built through MapReduce using Hadoop and stored in HDFS. Then, we import and transform the result in HDFS into Lucene indexes. Finally we can do full words search from Lucene (as web search).




