/**
 * Block Party Filesystem Node Client
 *
 * Example file that uses the bpfs procotol to connect to a
 * configured node on the network. This bypasses other arrangements
 * and uses the BpfsRCP object to communicate directly with nodes.
 */
package edu.rit.cs.examples;

import edu.rit.cs.*;
import java.util.ArrayList;

import java.io.IOException;
import java.net.InetSocketAddress;
import org.apache.hadoop.ipc.RPC;

public class BpfsNodeClient
{
	String[] hosts;
	BpfsConfiguration conf;

	public BpfsNodeClient()
		throws IOException
	{
		conf = new BpfsConfiguration();
		hosts = conf.getStrings(BpfsKeys.BPFS_CLIENT_NODES);
		if(hosts == null) { 
			System.err.println("No hosts configured for the client.");
			System.exit(1);
		}

		for(String hostaddr : hosts) {
			/* Add a proxy connection to the client */
			InetSocketAddress inetaddr = new InetSocketAddress(hostaddr,
							   conf.getInt(BpfsKeys.BPFS_NODE_PORT, 60010));

			BpfsRPC rpc = RPC.getProxy(BpfsRPC.class,
						   RPC.getProtocolVersion(BpfsRPC.class),
						   inetaddr,
						   conf);

			System.out.println("Sending ping out for " + inetaddr.toString());
			System.out.println(rpc.ping());
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

	/**
	 * Pings all of the nodes in the configuration file,
	 * prints out the output of the pings.
	 */
	public void pingNodes()
	{
	}

	public static void main(String[] args)
		throws IOException
	{
		BpfsNodeClient client;

		client = new BpfsNodeClient();
		client.printNodes();
	}
}
