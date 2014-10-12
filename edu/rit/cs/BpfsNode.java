/**
 * Block Party Filesystem Node Daemon
 *
 */
package edu.rit.cs;

import java.io.*;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

import org.apache.hadoop.ipc.RPC;

public class BpfsNode
	extends Configured
	implements BpfsRPC
{
	static
	{
		BpfsConfiguration.init();
	}

	private LinkedBlockingQueue workq;
	private BpfsConfiguration conf;
	private int port;
	private String addr;
	private final RPC.Server server;

	/**
	 * Ping "Am I alive method" or BpfsRPC
	 */
	@Override
	public String ping()
	{
		return "pong";
	}

	/**
	 * Returns the number of jobs in the work queue.
	 */
	@Override
	public int wqlen()
	{
		return workq.size();
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
	 * Get a block from the node, this method provides a key
	 * with which the data node will find wihthin the local 
	 * block repository.
	 */
	@Override
	public byte[] getBlock(String key)
	{
		return new byte[0];
	}

	/**
	 * Put a block into the node. This takes a block and simply
	 * stores the block as a contiguous file, with the filename
	 * the key given. In this implementation, it is expected that
	 * the filename be a SHA1 hash.
	 */
	public String putBlock(byte[] block)
	{
		return "0";
	}

	/**
	 * Sets the configuration file and any runtime constants that
	 * may need to be set.
	 */
	public BpfsNode()
		throws IOException
	{
		conf = new BpfsConfiguration();

		/* Get Configuration Objects */
		this.port = conf.getInt(BpfsKeys.BPFS_NODE_PORT, 60010);
		this.addr = conf.get(BpfsKeys.BPFS_NODE_ADDRESS, "0.0.0.0");
		this.workq = new LinkedBlockingQueue();

		/* Reaches out to the parent node conf */
		this.server = new RPC.Builder(conf).
			setInstance(this).
			setProtocol(BpfsRPC.class).
			setBindAddress(this.addr).
			setPort(this.port).
			build();

		/* Automatically start the node */
		this.server.start();
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
		throws IOException
	{
		BpfsNode node;

		node = new BpfsNode();
	}
}
