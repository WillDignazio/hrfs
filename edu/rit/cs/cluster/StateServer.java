/**
 * Copyright © 2014
 * State Server
 *
 * Server that listens for connections from other nodes,
 * designed specifically for receiving state objects. This
 * will wait a limited amount of time for state objects,
 * after which the state will be constructed from the received
 * objects.
 *
 * @author Will Dignazio <wdignazio@gmail.com>
 * @file StateServer.java
 */
package edu.rit.cs.cluster;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import edu.rit.cs.HrfsConfiguration;
import edu.rit.cs.HrfsKeys;

class StateServer
	extends Thread
{
	static
	{
		HrfsConfiguration.init();
	}

	private static final Log LOG = LogFactory.getLog(StateServer.class);
	private ClusterState state;
	private StateListener listener;
	private HrfsConfiguration conf;
	private ServerSocket stsock;
	private String host;
	private int port;

	/** Default constructor */
	public StateServer(StateListener listener)
	{
		if(listener == null) {
			LOG.fatal("Invalid State Listener");
			System.exit(1);
		}

		this.conf = new HrfsConfiguration();
		this.listener = listener;
		this.state = null;

		try {
			/* Random port, but given address */
			stsock = new ServerSocket(0);
		}
		catch(IOException e) {
			LOG.fatal("Unable to start state listener thread: " + e.toString());
			System.exit(1);
		}

	}

	/**
	 * Returns the host address of the listener socket.
	 * @return host Host Address of socket
	 */
	public String getListenerHostAddress()
	{
		String out;

		out = null;
		if(this.stsock == null)
			return null;

		/**
		 * In typical java form, there is not a good way to
		 * get a publicly available address. Admiteddly  this
		 * gets a little weird since the StateServer listens
		 * on all available addresses.
		 *
		 * So for this instance, we're going to use the same
		 * known address configured for the node.
		 */
		return conf.get(HrfsKeys.HRFS_NODE_ADDRESS,
				"127.0.0.1");
	}

	/**
	 * Reurns the host port of the listener socket.
	 * @return port Return port of listener socket
	 */
	public int getListenerPort()
	{
		if(this.stsock == null)
			return -1;

		return this.stsock.getLocalPort();
	}

	/**
	 * Run routine for the TCP state listener thread, when a new cluster state
	 * is sent, it will come through this listener.
	 */
	@Override
	public void run()
	{
		ClusterState istate;
		ObjectInputStream istream;
		Socket insock;
		long time;
		int cmp;

		cmp = 0;

		/* 
		 * We may try to load a state from disk, but if we
		 * don't, we need to make a dummy one.
		 */
		if(state == null) {
			LOG.info("Starting server with single node state");
			state = ClusterState.getSingleState();
			listener.newState(state);
		}

		for (;;) {
			try {
				/* Poll for socket connection */
				insock = stsock.accept();
				istream = new ObjectInputStream(insock.getInputStream());

				istate = (ClusterState)istream.readObject();
				if(state == null) {
					state = istate;
					listener.newState(state);
					continue;
				}

				/* If the remote is newer, change out state */
				cmp = state.compareTo(istate);
				if(cmp < 0) {
					state = istate;
					listener.newState(state);
				}
			}
			catch(ClassNotFoundException e) {
				LOG.warn("Invalid Cluster State received");
				continue;
			}
			catch(IOException e) {
				LOG.warn("Exception while accepting data from inbound cluster connection: " + e.toString());
				if(!stsock.isClosed()) {
					LOG.fatal("State server socket closed");
					System.exit(1);
				}
				continue;
			}
		}
	}
}
