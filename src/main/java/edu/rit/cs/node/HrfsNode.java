/**
 * Copyright © 2014
 * Hadoop Replicating Filesystem Node Daemon
 *
 * @file HrfsNode.java
 * @author Will Dignazio <wdignazio@gmail.com>
 */
package edu.rit.cs.node;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.cli.Options;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.HelpFormatter;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.ipc.RPC;

import org.apache.zookeeper.*;
import org.apache.zookeeper.ZooDefs.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import edu.rit.cs.HrfsRPC;
import edu.rit.cs.HrfsKeys;
import edu.rit.cs.HrfsConfiguration;
import edu.rit.cs.cluster.RingManager;

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
	private String address;
	private RPC.Server server;
	private RingManager cagent;

	/**
	 * By default, the HRFS Node will immediately use the local hrfs
	 * configuration to establish a listening socket. The HrfsNode uses the
	 * Hadoop RPC API to uphold the common HrfsRPC, and should faithfully
	 * respond to each request.
	 */
	public HrfsNode(Options options)
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
		this.address = conf.get(HrfsKeys.HRFS_NODE_ADDRESS, "0.0.0.0");
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
				LOG.info("Attempting to bind to port: " + port);

				this.server = new RPC.Builder(conf).
					setInstance(this).
					setProtocol(HrfsRPC.class).
					setBindAddress(this.address).
					setPort(this.port).
					build();
			}
			catch(BindException e) {
				LOG.warn("Unable to bind server to port: " + (port + p) +
					 e.toString());
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

		/* Build Ring Manager */
		this.cagent = new RingManager(
			new InetSocketAddress(address, port),
			options.hasOption("potent"));
		
		if(cagent.getRing() == null) {
			LOG.info("No Ring Exists!, new filesystem?");
		}

		/* Start Node Daemons */
		this.server.start();
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
	 * Helper help method, shows the available options and how to use them
	 * for the node. This method will call System.exit() with the provided
	 * integer exit argument.
	 * @param options Options
	 * @param exit Exit value for system
	 */
	private static void help(Options options, int exit)
	{
		HelpFormatter formatter;

		formatter = new HelpFormatter();
		formatter.printHelp("HrfsNode", options);
		System.exit(0);
	}
	
	/**
	 * Starts the HrfsNode daemon, this sets up the correct
	 * environment variables, and imports the default
	 * configurations from the config file.
	 */
	public static void main(String[] args)
		throws IOException
	{
		CommandLineParser parser;
		CommandLine cmdline;
		Options options;		
		HrfsNode node;


		try {
			parser = new BasicParser();			
			options = new Options();
			options.addOption("P", "potent", false, "Node is capable of creating ring.");
			options.addOption("h", "help", false, "Show this help dialogue.");

			cmdline = parser.parse(options, args);
			if(cmdline.hasOption("h"))
				help(options, 0);
			
			node = new HrfsNode(options);
		}
		catch(ParseException e) {
			LOG.error("Failed to parse arguments of node: " + e.getMessage());
		}
	}
}
