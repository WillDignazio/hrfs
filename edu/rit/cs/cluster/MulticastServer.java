/**
 * Copyright © 2014
 * Hadoop Replicating Filesystem Cluster Agent
 *
 * Server that listens on the multicast address for datagrams
 * relaying cluster information. In particular this is announcements
 * and removals from the cluster.
 */
package edu.rit.cs.cluster;

import java.io.*;
import java.net.*;
import java.util.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import edu.rit.cs.cluster.MulticastListener;
import edu.rit.cs.HrfsConfiguration;
import edu.rit.cs.HrfsKeys;

class MulticastServer
	extends Thread
{
	static
	{
		HrfsConfiguration.init();
	}

	private static final Log LOG = LogFactory.getLog(MulticastServer.class);
	private static int MAX_UDP_SIZE	= 65507; // Max UDP packet size in bytes

	private PrintWriter swriter;
	private MulticastSocket socket;
	private InetAddress group;
	private String addr;
	private int port;

	private HrfsConfiguration conf;
	private MulticastListener listener;

	/**
	 * On startup joins the multicast group, however delays
	 * announcement of the node until the server has been started.
	 */
	public MulticastServer(MulticastListener listener)
	{
		if(listener == null) {
			LOG.fatal("Invalid multicast listener");
			System.exit(1);
		}

		this.conf = new HrfsConfiguration();
		this.addr = conf.get(HrfsKeys.HRFS_NODE_GROUP_ADDRESS, "224.0.1.150");
		this.port = conf.getInt(HrfsKeys.HRFS_NODE_GROUP_PORT, 1246);
		this.listener = listener;

		try {
			this.socket = new MulticastSocket(this.port);
			this.group = InetAddress.getByName(this.addr);
			socket.joinGroup(group);
		}
		catch(UnknownHostException e) {
			LOG.fatal("Unknown group address: " +
				  e.toString());
			System.exit(1);
		}
		catch(IOException e) {
			LOG.fatal("Failed to initialize cluster connection: " +
				  e.toString());
			System.exit(1);
		}
	}

	/**
	 * Run routine for the multicast listener thread, handles
	 * incoming multicast messages to the cluster, and relays
	 * them to the agent.
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
				break;
			}

			data = packet.getData();
			if(data == null)
				break;

			smsg = new String(data).split("!");
			if(smsg == null)
				continue;

			/**
			 * The protocol for multiast communication is simple, and
			 * is used primarily for initial communication.
			 */
			switch(smsg[0])
			{
			case "announce":
				LOG.info("Got announce: " + new String(data));
				listener.newNode(smsg[1], Integer.parseInt(smsg[2].trim()));
				break;
			case "join":
				LOG.info("Got join: " + new String(data));
				listener.nodeJoin(smsg[1], Integer.parseInt(smsg[2].trim()));
				break;
			default:
				LOG.warn("Unknown cluster command: " + smsg[0]);
				continue;
			}
		}

		/* Hit error or close call, try to cleanup */
		LOG.error("Group socket closed");
		socket.close();
	}

	/**
	 * Announce to the cluster that the node wishes to join.
	 * @param host Host address of this node for state connection.
	 * @param port Host port open on this node for state connection.
	 */
	public synchronized void announce(String host, int port)
	{
		DatagramPacket packet;
		String out;
		byte[] buf;

		try {
			/* XXX Endianness might be a problem */
			out = "announce!" + host + "!" + port;
			packet = new DatagramPacket(out.getBytes(), out.length(), this.group, this.port);
			socket.send(packet);
		}
		catch(IOException e) {
			LOG.error("Failed to announce node join: " + e.toString());
		}
	}

	/**
	 * Inform the cluster that we wish to join, and share the ring space
	 * with the rest of the nodes.
	 * @param host Address from which a node can receive the new state from
	 * @param port Port from which a node can receive the new state from
	 */
	public synchronized void join(String host, int port)
	{
		DatagramPacket packet;
		String out;
		byte[] buf;

		try {
			/* XXX Endianness */
			out = "join!" + host + "!" + port;
			packet = new DatagramPacket(out.getBytes(), out.length(), this.group, this.port);
			socket.send(packet);
		}
		catch(IOException e) {
			LOG.error("Failed to send node join request: " + e.toString());
		}
	}
}
