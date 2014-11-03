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
 *
 * @author Will Dignazio <wdignazio@gmail.com>
 * @file ClusterAgent.java
 */
package edu.rit.cs.cluster;

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

import edu.rit.cs.HrfsConfiguration;
import edu.rit.cs.HrfsKeys;

public class ClusterAgent
	implements StateListener
{
	static
	{
		HrfsConfiguration.init();
	}

	private static final Log LOG = LogFactory.getLog(ClusterAgent.class);
	private static int MAX_UDP_SIZE	= 65507; // Max UDP packet size in bytes
	private static int THREAD_POOL_SIZE = 5;

	private HrfsConfiguration conf;
	private PrintWriter swriter;
	private MulticastSocket socket;
	private InetAddress group;
	private MulticastListener listener;
	private String addr;
	private int port;

	private ClusterClient client;
	private ClusterState state;
	private	StateServer serv;

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
		this.serv = new StateServer(this);

		try {
			this.socket = new MulticastSocket(this.port);
			this.group = InetAddress.getByName(this.addr);

			LOG.info("Joining cluster on mulitcast group: " + group.toString());
			socket.joinGroup(group);

			/* Start a listener thread on the socket. */
			LOG.info("Starting cluster multicast listener thread");
			this.listener = new MulticastListener();
			this.listener.start();

			/* Start the State Server */
			this.serv.start();

			/* Send out announcement of join */
			announce();
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
		 * Run routine for the multicast listener thread, handles incoming multicast messages
		 * to the cluster, and relays them to the agent.
		 *
		 * XXX Log messages are not _always_ shown, even when you use the LogFactory to create
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
					break;
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
					LOG.info("Got announce: " + new String(data));
					sendState(smsg[1], Integer.parseInt(smsg[2].trim()));
					break;
				case "join":
					LOG.info("Got join: " + new String(data));
					
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
	 * Send the state to a remote host given at the address and port.
	 * This is typically after an announcement has been picked up from
	 * the MulticastListener.
	 */
	public synchronized void sendState(String host, int port)
	{
		Socket osock;
		ObjectOutputStream ostream;

		if(this.state == null) {
			LOG.warn("Cluster state null, aborting sendState, node in construction");
			return; // Just quit here
		}

		try {
			osock = new Socket(host, port);
			osock.setSoTimeout(1000 * 10); // 10 Seconds

			ostream = new ObjectOutputStream(osock.getOutputStream());
			ostream.writeObject(this.state);

			ostream.flush();
			osock.close();
			LOG.info("Sent state to recipient: " + host + ":" + port);
		}
		catch(UnknownHostException e) {
			LOG.error("Unable to resolve state recipient: " + host);
		}
		catch(IOException e) {
			LOG.error("Failed to send state to recipient.");
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

		try {
			cport = serv.getListenerPort();
			caddr = serv.getListenerHostAddress();

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

	/**
	 * Implementation of newState, the State server has received and given us a new
	 * state for the cluster.
	 */
	public void newState(ClusterState state)
	{
		LOG.info("ClusterAgent received new state: " + state.getTimestamp());
		this.state = state;
	}
}
