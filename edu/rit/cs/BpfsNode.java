/**
 * Block Party Filesystem Node Daemon
 *
 */
package edu.rit.cs;

import java.io.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class BpfsNode
	extends Configured
	implements BpfsRPC
{

	static
	{
		BpfsConfiguration.init();
	}

	BpfsConfiguration conf;

	private int port;
	private String addr;

	/**
	 * Sets the configuration file and any runtime constants that
	 * may need to be set.
	 */
	public BpfsNode()
	{
		conf = new BpfsConfiguration();

		this.port = conf.getInt(BpfsKeys.BPFS_NODE_PORT, 60010);
		this.addr = conf.get(BpfsKeys.BPFS_NODE_ADDRESS, "0.0.0.0");
	}

	/**
	 * Implements the method for configured, returns the 
	 * BpfsConfiguration that is installed within the node.
	 */
	@Override
	public Configuration getConf()
	{
		return this.conf;
	}

	/**
	 * Implements the method for configured, sets the
	 * configuration file for the node.
	 */
	@Override
	public void setConf(Configuration conf)
	{
		this.conf = (BpfsConfiguration)conf;
	}

	/**
	 * Starts the BpfsNode daemon, this sets up the correct
	 * environment variables, and imports the default
	 * configurations from the config file.
	 */
	public static void main(String[] args)
	{
		BpfsNode node;

		node = new BpfsNode();
	}
}
