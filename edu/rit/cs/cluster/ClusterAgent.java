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
	implements StateListener, MulticastListener
{
	static
	{
		HrfsConfiguration.init();
	}

	private static final Log LOG = LogFactory.getLog(ClusterAgent.class);
	private static int THREAD_POOL_SIZE = 5;

	private HrfsConfiguration conf;
	private ClusterState state;
	private	StateServer stserv;
	private MulticastServer mcserv;

	/**
	 * Build the node cluster agent, listen on the configured multicast
	 * group from Hadoop. This will be the network that the proxy listens
	 * for cluster anouncements and configuration changes.
	 */
	public ClusterAgent()
	{
		this.conf = new HrfsConfiguration();
		this.stserv = new StateServer(this);
		this.mcserv = new MulticastServer(this);

		/* Start our subservient daemons */
		this.mcserv.start();
		this.stserv.start();

		/* Announce presence on network */
		mcserv.announce(stserv.getListenerHostAddress(),
				stserv.getListenerPort());

		/* I want to join the cluster */
		mcserv.join(stserv.getListenerHostAddress(),
			    stserv.getListenerPort());
	}

	/**
	 * Handles when a node has announced itself on the network, and needs to
	 * be introduced into the cluster.
	 * @param host Host address of the node.
	 * @param port Port of the node.
	 */
	@Override
	public void newNode(String host, int port)
	{
		stserv.sendState(host, port);
	}

	/**
	 * A node is joining the cluster, and we need to fetch a new state from them.
	 * This state will supposedly be the new state of the cluster.
	 * @param host Host to receive new state from
	 * @param port Port to receive new state from
	 */
	@Override
	public void nodeJoin(String host, int port)
	{
		stserv.recvState(host, port);
	}

	/**
	 * Implementation of newState, the State server has received and given us a new
	 * state for the cluster.
	 */
	@Override
	public void newState(ClusterState state)
	{
		LOG.info("ClusterAgent received new state: " + state.getTimestamp());
		this.state = state;
	}
}
