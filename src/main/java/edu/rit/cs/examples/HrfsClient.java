package edu.rit.cs.examples;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import java.net.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.ipc.RPC;

import edu.rit.cs.*;

public class HrfsClient
{
	public static void main(String[] args)
		throws Exception
	{
		FileSystem fs;
		HrfsConfiguration conf;
		InetSocketAddress addr;

		if(args.length != 1) {
			System.err.println("Usage: hadoop jar hrfs.jar edu.cs.rit.HrfsClient <file>");
			System.exit(1);
		}
		
		conf = new HrfsConfiguration();
		fs = new Hrfs();
	}
}
