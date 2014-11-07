/**
 * Copyright © 2014
 * Hadoop Replicating Filesystem Node Daemon
 *
 * @file HrfsNode.java
 * @author Will Dignazio <wdignazio@gmail.com>
 */
package edu.rit.cs;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.ipc.RPC;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import edu.rit.cs.HrfsRing;
import edu.rit.cs.cluster.ClusterAgent;

public class HrfsNode
	implements HrfsRPC
{
	static
	{
		HrfsConfiguration.init();
	}

	private static int MAX_PORT_OFFSET = 100; // Maximum difference from conf port
	private static final Log LOG = LogFactory.getLog(HrfsNode.class);

	private File datadir;
	private LinkedBlockingQueue workq;
	private HrfsConfiguration conf;
	private int port;
	private String addr;
	private RPC.Server server;
	private ClusterAgent cagent;

	/**
	 * By default, the HRFS Node will immediately use the local hrfs
	 * configuration to establish a listening socket. The HrfsNode uses
	 * the Hadoop RPC API to uphold the common HrfsRPC, and should faithfully
	 * respond to each request.
	 */
	public HrfsNode()
		throws IOException
	{
		server = null;
		conf = new HrfsConfiguration();
		datadir = new File(conf.get(HrfsKeys.HRFS_NODE_PATH));

		/* Check that the data dir exists */
		if(!datadir.exists() || !datadir.isDirectory()) {
			LOG.fatal("Node data path is not a directory");
			System.exit(1);
		}

		/* Get Configuration Objects */
		this.port = conf.getInt(HrfsKeys.HRFS_NODE_PORT, 60010);
		this.addr = conf.get(HrfsKeys.HRFS_NODE_ADDRESS, "0.0.0.0");
		this.workq = new LinkedBlockingQueue();

		/*
		 * In a scenario with multiple nodes running on a server, we are
		 * going to increment the port that they listen on.
		 *
		 * Each node will have MAX_PORT_OFFSET number of tries to claim
		 * a port on the system. If it can't (there is alot of disks), the
		 * node will throw a fatal error and terminate.
		 */
		for(int p=0; p < MAX_PORT_OFFSET; ++p) {
			try {
				if(this.server != null)
					break;

				this.port = this.port + p;
				this.server = new RPC.Builder(conf).
					setInstance(this).
					setProtocol(HrfsRPC.class).
					setBindAddress(this.addr).
					setPort(this.port).
					build();

			}
			catch(BindException e) {
				LOG.warn("Unable to bind server to port: " + (port + p));
				continue;
			}
			catch(Exception e) {
				LOG.fatal("Failed to initialize RPC server: " + e.getMessage());
				System.exit(1);
			}
		}

		/* Most likely a logic flaw */
		if(this.server == null) {
			LOG.error("Invalid RPC Server");
			System.exit(1);
		}

		/* Build cluster proxy */
		this.cagent = new ClusterAgent(this);

		/* Start Node Daemons */
		this.server.start();
	}

	/**
	 * Gets the port this hrfs node will accept rpc calls from.
	 * @return rpc Hadoop RPC server port
	 */
	public int getRPCServerPort()
	{
		return this.port;
	}

	/**
	 * Ping "Am I alive method" or HrfsRPC
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

	/** Get a block from the node */
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
		NodeWriter writer;

		out = null;
		try {
			writer = new NodeWriter(conf.get(HrfsKeys.HRFS_NODE_PATH));
			writer.write(new String(block).toCharArray());
			writer.close();

			if(writer.isPlaced())
				out = writer.blockName();
		}
		catch(FileNotFoundException e) {
			LOG.error("Something seems to have happened to the data directory: "
				  + e.toString());
			return null;
		}
		catch(IOException e) {
			LOG.error("Failed to write block to node: " + e.toString());
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
	 * Starts the HrfsNode daemon, this sets up the correct
	 * environment variables, and imports the default
	 * configurations from the config file.
	 */
	public static void main(String[] args)
		throws IOException
	{
		HrfsNode node;

		node = new HrfsNode();
	}
}
