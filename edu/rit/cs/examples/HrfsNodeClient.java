/**
 * Copyright © 2014
 * Hadoop Replicating Filesystem Node Client
 *
 * Example file that uses the hrfs procotol to connect to a
 * configured node on the network. This bypasses other arrangements
 * and uses the HrfsRCP object to communicate directly with nodes.
 */
package edu.rit.cs.examples;

import edu.rit.cs.*;
import java.util.ArrayList;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import org.apache.hadoop.ipc.RPC;

public class HrfsNodeClient
{
	HrfsConfiguration conf;

	public HrfsNodeClient()
		throws IOException
	{
		conf = new HrfsConfiguration();
	}
}
