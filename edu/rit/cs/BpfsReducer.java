/**
 * Copyright © 2014
 * Block Party Filesystem Reducer
 *
 * Reducer: [(Blk#, Blk)...] => [(Blk#, Sha1)...]
 */
package edu.rit.cs;

import java.io.*;
import java.util.Iterator;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class BpfsReducer
	extends MapReduceBase
	implements Reducer<LongWritable, BytesWritable, LongWritable, Text>
{
	@Override
	public void reduce(LongWritable blk, Iterator<BytesWritable> values,
			   OutputCollector<LongWritable, Text> output,
			   Reporter reporter)
	{
		System.out.println(blk);
	}
}
		
