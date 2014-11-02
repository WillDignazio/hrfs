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

public class HrfsNode
	extends Configured
	implements HrfsRPC
{
	public static int MAX_PORT_OFFSET = 100; // Maximum difference from conf port
	public static int MAX_UDP_SIZE	= 65507; // Max UDP packet size in bytes

	private static final Log LOG = LogFactory.getLog(HrfsNode.class);

	static
	{
		HrfsConfiguration.init();
	}

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

				this.server = new RPC.Builder(conf).
					setInstance(this).
					setProtocol(HrfsRPC.class).
					setBindAddress(this.addr).
					setPort(this.port + p).
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
		this.cagent = new ClusterAgent();

		/* Start Node Daemons */
		this.server.start();
		this.cagent.start();
	}

	/**
	 * Representative of the node to the multicast cluster group, allows
	 * for the group of nodes to make placements and ring adjustments
	 * on behalf of the node.
	 *
	 * This also runs as a seperate server thread within the Node, and
	 * acts on a typical multicast group network.
	 */
	private class ClusterAgent
		extends Thread
	{
		private MulticastSocket socket;
		private InetAddress group;

		/**
		 * Build the node cluster agen, listen on the configured multicast
		 * group from Hadoop. This will be the network that the proxy listens
		 * for cluster anouncements and configuration changes.
		 */
		public ClusterAgent()
		{
			try {
				this.socket = new MulticastSocket(
					conf.getInt(HrfsKeys.HRFS_NODE_GROUP_PORT, 1246));
				this.group = InetAddress.getByName(
					conf.get(HrfsKeys.HRFS_NODE_GROUP_ADDRESS, "224.0.1.150"));

				LOG.info("Joining cluster on mulitcast group: " + group.toString());
				socket.joinGroup(group);
			}
			catch(UnknownHostException e) {
				LOG.fatal("Unknown group address: " +
					  e.toString());
			}
			catch(IOException e) {
				LOG.fatal("Failed to initialize cluster connection: " +
					  e.toString());
				System.exit(1);
			}
		}

		/** Running Thread */
		public void run()
		{
			byte[] buffer;
			DatagramPacket packet;
			int nbytes;
			byte[] data;
			String[] smsg;

			nbytes = 0;
			buffer = new byte[MAX_UDP_SIZE];
			packet = null;
			smsg = null;

			for(;;) {
				packet = new DatagramPacket(buffer, buffer.length);

				try {
					socket.receive(packet);
				}
				catch(IOException e) {
					LOG.warn("Error receiving cluster data: " + e.getMessage());
				}

				data = packet.getData();
				if(data == null) {
					LOG.error("Group socket closed");
					break;
				}
				
				smsg = new String(data).split("!");
				if(smsg == null) {
					LOG.warn("Received invalid cluster packet");
					continue;
				}

				/**
				 * The protocol for multiast communication is simple, and
				 * is used primarily for initial communication.
				 */
				switch(smsg[0])
				{
				case "announce":
					if(smsg.length != 2)
						break;
					LOG.info("Got announce from " + smsg[0]);
					break;
				default:
					LOG.warn("Unknown cluster command: " + smsg[0]);
					continue;
				}
			}

			/* Hit error or close call, try to cleanup */
			socket.close();
		}
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

	/**
	 * Implements the method for configured, returns the 
	 * HrfsConfiguration that is installed within the node.
	 */
	@Override
	public Configuration getConf()
	{
		return this.conf;
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
	 * Implements the method for configured, sets the
	 * configuration file for the node.
	 * @param conf Configuration object
	 */
	@Override
	public void setConf(Configuration conf)
	{
		this.conf = (HrfsConfiguration)conf;
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
