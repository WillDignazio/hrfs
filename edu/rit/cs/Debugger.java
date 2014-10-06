/**
 * Test Debugger Class
 *
 * @author William Dignazio <wmd4589@cs.rit.edu>
 * @file Debugger.java
 * @version 10/03/2014
 */
package edu.rit.cs;

import org.apache.hadoop.hdfs.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

import java.io.*;
import java.util.*;
import java.net.*;

public class Debugger
{
	public static void main(String[] args)
	{
		BlockPartyClient client;
		Configuration conf;
		DistributedFileSystem fs;
		Path path;
		String host;
		Iterator<Map.Entry<String,String>> iter;


		if(args.length != 1) {
			System.err.println("Usage: hadoop jar blockparty.jar edu.rit.cs.Debugger <host>");
			System.exit(1);
		}

		host = args[0];

		try {
			conf = new Configuration();
			fs = (DistributedFileSystem)FileSystem.get(conf);

			iter = conf.iterator();
			while(iter.hasNext())
				System.out.println(iter.next());

			System.out.println("FS Class: " + fs.getClass());
			path = new Path("hdfs://" + host + ":9000/");
			System.out.println("Configuration Keys: " + conf.size());
			System.out.println("Root Path Exists: " + fs.exists(path));
			path = new Path("hdfs://" + host + ":9000/README");
			FileMetadata fm = new FileMetadata(fs, path);
			System.out.println(fm.replicaCount());
		}
		catch(IOException e) {
			System.err.println("Failed to initialize debugger");
			System.exit(1);
		}
	}
}
