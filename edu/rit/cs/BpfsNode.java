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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.ipc.RPC;

public class BpfsNode
	extends Configured
	implements BpfsRPC
{
	File datadir;
	Log log;

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
	 * Get a block from the node */
	@Override
	public byte[] getBlock(String key)
	{
		return new byte[0];
	}

	/** Put a block into the node. */
	@Override
	public String putBlock(byte[] block)
	{
		String out;
		BlockWriter writer;
		
		out = null;
		try {
			writer = new BlockWriter(conf.get(BpfsKeys.BPFS_NODE_PATH));
			writer.write(new String(block).toCharArray());
			writer.close();

			if(writer.isPlaced())
				out = writer.blockName();
		}
		catch(FileNotFoundException e) {
			log.error("Something seems to have happened to the data directory: "
				  + e.toString());
			return null;
		}
		catch(IOException e) {
			log.error("Failed to write block to node: " + e.toString());
			return null;
		}

		return out;
	}

	/** Removes the block from the node. */
	@Override
	public boolean delBlock(String key)
	{
		return false;
	}

	/**
	 * Sets the configuration file and any runtime constants that
	 * may need to be set.
	 */
	public BpfsNode()
		throws IOException
	{

		conf = new BpfsConfiguration();
		log = LogFactory.getLog(BpfsNode.class);
		datadir = new File(conf.get(BpfsKeys.BPFS_NODE_PATH));

		/* Check that the data dir exists */
		if(!datadir.exists() || !datadir.isDirectory()) {
			log.error("Node data path is not a directory");
			System.exit(1);
		}

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
