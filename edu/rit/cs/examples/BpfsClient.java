package edu.rit.cs.examples;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import java.net.URI;
import java.net.URISyntaxException;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

import edu.rit.cs.*;

public class BpfsClient
{
	public static void main(String[] args)
	{
		Bpfs fs;
		Configuration conf;

		try {
			conf = new Configuration();
			fs = new Bpfs(new URI("bpfs://batou.local"), conf);
		}
		catch(URISyntaxException e) {
			System.err.println("Invalid URI.");
			System.exit(1);
		}
	}
}
