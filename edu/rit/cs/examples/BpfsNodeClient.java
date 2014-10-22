/**
 * Copyright © 2014
 * Block Party Filesystem Node Client
 *
 * Example file that uses the bpfs procotol to connect to a
 * configured node on the network. This bypasses other arrangements
 * and uses the BpfsRCP object to communicate directly with nodes.
 */
package edu.rit.cs.examples;

import edu.rit.cs.*;
import java.util.ArrayList;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import org.apache.hadoop.ipc.RPC;

public class BpfsNodeClient
{
	String[] hosts;
	BpfsConfiguration conf;
	ArrayList<BpfsRPC> rpcs;

	public BpfsNodeClient()
		throws IOException
	{
		conf = new BpfsConfiguration();
		hosts = conf.getStrings(BpfsKeys.BPFS_CLIENT_NODES);
		rpcs = new ArrayList<BpfsRPC>();

		if(hosts == null) { 
			System.err.println("No hosts configured for the client.");
			System.exit(1);
		}

		for(String hostaddr : hosts) {
			/* Add a proxy connection to the client */
			InetSocketAddress inetaddr = new InetSocketAddress(hostaddr,
							   conf.getInt(BpfsKeys.BPFS_NODE_PORT, 60010));

			rpcs.add(RPC.getProxy(BpfsRPC.class,
					      RPC.getProtocolVersion(BpfsRPC.class),
					      inetaddr,
					      conf));
		}
	}

	/**
	 * Prints out the given Nodes configured locally.
	 */
	public void printNodes()
	{
		for(String host : this.hosts)
			System.out.println("Host: " + host);
	}

	/** Write N random blocks to target node */
	public void writeBlocks(int nblocks)
		throws IOException
	{
		FileInputStream fis;
		byte[] buff;	
		String out;
		int count;

		fis = new FileInputStream("/dev/urandom");
		buff = new byte[512];
		for(BpfsRPC rpc : rpcs) {
			for(int b=0; b < nblocks; ++b) {
				count = fis.read(buff);
				out = rpc.putBlock(buff);
				System.out.println("Wrote : " + out);
			}
		}
	}

	/**
	 * Pings all of the nodes in the configuration file,
	 * prints out the output of the pings.
	 */
	public void pingNodes()
	{
		for(BpfsRPC rpc : rpcs)
			System.out.println(rpc.ping());
	}

	public static void main(String[] args)
		throws IOException
	{
		BpfsNodeClient client;

		client = new BpfsNodeClient();
		client.printNodes();
		client.pingNodes();
		client.writeBlocks(10);
	}
}
