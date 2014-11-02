/**
 * Copyright © 2014
 * Hadoop Replicating Filesystem Cluster Agent
 * 
 * Representative to a multicast cluster group, allows
 * for a group of nodes to make placements and ring adjustments
 * on behalf of a node.
 *
 * This also runs as a seperate server thread within the Node, and
 * acts on a typical multicast group network.
 */
package edu.rit.cs;

import java.io.*;
import java.net.*;
import java.util.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class ClusterAgent
{
	static
	{
		HrfsConfiguration.init();
	}

	private static final Log LOG = LogFactory.getLog(ClusterAgent.class);
	private static int MAX_UDP_SIZE	= 65507; // Max UDP packet size in bytes

	private HrfsConfiguration conf;
	private PrintWriter swriter;
	private MulticastSocket socket;
	private InetAddress group;
	private MulticastListener listener;
	private ClusterClient client;
	private String addr;
	private int port;

	/**
	 * Build the node cluster agent, listen on the configured multicast
	 * group from Hadoop. This will be the network that the proxy listens
	 * for cluster anouncements and configuration changes.
	 */
	public ClusterAgent(ClusterClient client)
	{
		if(client == null) {
			LOG.fatal("Invalid cluster client");
			System.exit(1);
		}

		this.client = client;
		this.conf = new HrfsConfiguration();
		this.addr = conf.get(HrfsKeys.HRFS_NODE_GROUP_ADDRESS, "224.0.1.150");
		this.port = conf.getInt(HrfsKeys.HRFS_NODE_GROUP_PORT, 1246);

		try {
			this.socket = new MulticastSocket(this.port);
			this.group = InetAddress.getByName(this.addr);

			LOG.info("Joining cluster on mulitcast group: " + group.toString());
			socket.joinGroup(group);

			/* Start a listener thread on the socket. */
			LOG.info("Starting cluster multicast listener thread");
			this.listener = new MulticastListener();
			this.listener.start();
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

		/* Send out announcement of join */
		LOG.info("Sending new cluster agent announce");
		announce();
	}

	/**
	 * Worker thread that listens on the multicast address for datagrams
	 * relaying cluster information. In particular this is announcements
	 * and removals from the cluster.
	 */
	private class MulticastListener
		extends Thread
	{
		/* Default Constructor */
		public MulticastListener()
		{
		}

		/**
		 * Run routine for the listener thread, handles incoming multicast messages
		 * to the cluster, and relays them to the agent.
		 *
		 * XXX Log messages are not shown, even when you use the LogFactory to create
		 * 	a new one specifically for the inner class. They are present for when
		 *	a solution has been found.
		 */
		@Override
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
					LOG.info("Got announce from " + smsg[1]);
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
	 * Announce to the cluster that the node wishes to join.
	 */
	public synchronized void announce()
	{
		DatagramPacket packet;
		int cport;
		String caddr;
		String out;
		byte[] buf;

		cport = client.getHostPort();
		caddr = client.getHostAddress();

		try {
			/* XXX Endianness might be a problem */
			out = "announce!" + caddr + "!" + cport;
			packet = new DatagramPacket(out.getBytes(), out.length(), this.group, this.port);
			socket.send(packet);
		}
		catch(UnsupportedEncodingException e) {
			LOG.error("US-ASCII unsupported, too old for commodity hardware...");
		}
		catch(IOException e) {
			LOG.error("Failed to announce node join: " + e.toString());
		}
	}
}
