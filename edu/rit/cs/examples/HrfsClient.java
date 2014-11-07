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
		HrfsWriter writer;
		InetSocketAddress addr;
		HrfsRPC rpc;

		if(args.length != 1) {
			System.err.println("Usage: hadoop jar hrfs.jar edu.cs.rit.HrfsClient <file>");
			System.exit(1);
		}
		
		conf = new HrfsConfiguration();
		addr = new InetSocketAddress(conf.get(HrfsKeys.HRFS_NODE_ADDRESS, "127.0.0.1"),
				       conf.getInt(HrfsKeys.HRFS_NODE_PORT, 60010));
		
		rpc = RPC.getProxy(HrfsRPC.class,
					   RPC.getProtocolVersion(HrfsRPC.class),
					   addr, conf);

		writer = new HrfsWriter(args[0], rpc);
		writer.publish();
	}
}
