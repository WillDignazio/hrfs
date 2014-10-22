/**
 * Block Party Filesystem Block Map
 *
 */
package edu.rit.cs.examples;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

import edu.rit.cs.Bpfs;
import edu.rit.cs.BpfsMapper;
import edu.rit.cs.BpfsReducer;

public class BpfsMapTest
{
	public static void main(String[] args)
		throws IOException, URISyntaxException
	{
		Bpfs fs;
		JobConf jconf;

		if(args.length != 1) {
			System.err.println("Usage: hadoop jar edu.cs.rit.BpfsMapTest <file>");
			System.exit(1);
		}

		fs = new Bpfs();
		jconf = new JobConf(BpfsMapTest.class);
		jconf.setJobName("blockmap");

		jconf.setOutputKeyClass(LongWritable.class);
		jconf.setOutputValueClass(BytesWritable.class);

		jconf.setMapperClass(BpfsMapper.class);

		FileInputFormat.addInputPath(jconf, new Path("file:/"+args[0]));
		FileOutputFormat.setOutputPath(jconf, new Path("bpfs://batou.local/" + args[0]+"-output"));
		JobClient.runJob(jconf);
	}
}
