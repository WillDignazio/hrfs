/**
 * BlockParty Client
 *
 */
package edu.rit.cs;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

import java.io.*;
import java.util.*;
import java.net.*;

public class BlockPartyClient
{
	FileSystem fs;
	Configuration conf;

	/**
	 * Default constructor, using the hadoop API, grabs the default
	 * configuration fields from the $HADOOP_HOME.
	 */
	public BlockPartyClient()
	{
		try {
			conf = new Configuration();
			fs = FileSystem.get(new Configuration());
		}
		catch(IOException e) {
			System.out.println("Failed to build BlockParty client");
			System.exit(1);
		}
	}

	/**
	 * Main method, starts the client application,
	 * and starts the process of handling client
	 * data.
	 */
	public static void main(String[] args)
	{
		BlockPartyClient party;

		party = new BlockPartyClient();
	}
}
