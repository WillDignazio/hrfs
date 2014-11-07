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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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

	private static final int THREAD_POOL_SIZE = 10;

	private static final Log LOG = LogFactory.getLog(StateServer.class);
	private ExecutorService executor;
	private ClusterState state;
	private PersistentSendServer pserv;
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
		this.executor = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
		this.pserv = new PersistentSendServer();

		this.state = null;
		this.stsock = null;
		this.host = null;
		this.port = -1;

		try {
			/* Random port, but given address */
			stsock = new ServerSocket(0);
			this.pserv.start();
		}
		catch(IOException e) {
			LOG.fatal("Unable to start state listener thread: " + e.toString());
			System.exit(1);
		}
	}

	/**
	 * Sets the internal state the server shall serve, this could be
	 * for a new cluster state, or a fix to broken ring.
	 */
	public void setState(ClusterState istate)
	{
		synchronized(this.state) {
			if(this.state.compareTo(istate) > 0) {
				LOG.error("Internal state newer than given state setState parameter");
				return;
			}

			this.state = istate;
		}
				
	}

	/**
	 * Send the state to a remote host given at the address and port.
	 * This is typically after an announcement has been picked up from
	 * the MulticastListener.
	 */
	public synchronized void sendState(String host, int port)
	{
		this.executor.execute(new SendStateServlet(host, port));
	}

	/**
	 * Receive the state from a remote host given at the address and port.
	 * An example is when a node joins the cluster and wishes to share the new
	 * ring state.
	 */
	public synchronized void recvState(String host, int port)
	{
		this.executor.execute(new RecvStateServlet(host, port));
	}

	/**
	 * Servlet that can be used to asynchronously receive a state
	 * from another host. Used in various scenarios, such as when a
	 * new node wishes to join, or drop from the cluster.
	 */
	private class RecvStateServlet
		implements Runnable
	{
		private String host;
		private int port;

		public RecvStateServlet(String host, int port)
		{
			this.host = host;
			this.port = port;
		}

		public void run()
		{
			Socket isock;
			ObjectInputStream istream;

			try {
				isock = new Socket(host, port);
				isock.setSoTimeout(1000 * 10); // 10 Seconds

				istream = new ObjectInputStream(isock.getInputStream());

				synchronized(state) {
					state = (ClusterState)istream.readObject();
					listener.newState(state);
					isock.close();
				}

				LOG.info("Received state (" + state.getTimestamp() +
					 " from " + host + ":" + port + ")");
			}
			catch(UnknownHostException e) {
				LOG.error("Unabe to resolve state server: " + host + ":" + port);
			}
			catch(ClassNotFoundException e) {
				LOG.error("Invalid state received from server: " + host + ":" + port);
			}
			catch(IOException e) {
				LOG.error("Failed to receive state from server: " + host +  ":" + port);
			}
		}
	}

	/**
	 * Servlet that can be used to asychronously send a state
	 * to another host. This is generally used by an executor
	 * service that manages a thread pool of them.
	 */
	private class SendStateServlet
		implements Runnable
	{
		private String host;
		private int port;

		public SendStateServlet(String host, int port)
		{
			this.host = host;
			this.port = port;
		}

		public void run()
		{
			Socket osock;
			ObjectOutputStream ostream;

			if(state == null)
				return; // Just quit here

			try {
				osock = new Socket(host, port);
				osock.setSoTimeout(1000 * 10); // 10 Seconds

				ostream = new ObjectOutputStream(osock.getOutputStream());
				synchronized(state) {
					ostream.writeObject(state);
					ostream.flush();
					osock.close();
				}

				LOG.info("Sent state to recipient: " + host + ":" + port);
			}
			catch(UnknownHostException e) {
				LOG.error("Unable to resolve state recipient: " + host);
			}
			catch(IOException e) {
				LOG.error("Failed to send state to recipient.");
			}
		}
	}

	/**
	 * Gets the port that is serving the nodes current state.
	 * @return port Port that state can be retrieved from.
	 */
	public int getServerPort()
	{
		return this.pserv.getPort();
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
	 * Persistent sending thread that upon request sends the state to the accepted client.
	 */
	private class PersistentSendServer
		extends Thread
	{
		ObjectOutputStream ostream;
		ServerSocket psock;

		public PersistentSendServer()
		{
			try {
				this.psock = new ServerSocket(0);
				LOG.info("Persistent server port: " + this.psock.getLocalPort());
			}
			catch(IOException e) {
				LOG.error("Failed to create persistent state server: " +
					  e.toString());
				System.exit(1);
			}
		}

		/**
		 * Returns the port that the persistent server is willing to send
		 * the State to.
		 * @return port Port one can retrieve this nodes state from
		 */
		public int getPort()
		{
			if(this.psock == null) {
				LOG.error("Persistent State Server not initialized");
				return -1;
			}
			return this.psock.getLocalPort();
		}
		
		/**
		 * Constantly listens for connections asking for the state
		 * of the cluster. This responds with the cluster state.
		 */
		@Override
		public void run() {
			for(;;) {
				try {
					Socket osock;

					osock = psock.accept();
					ostream = new ObjectOutputStream(osock.getOutputStream());
					synchronized(state) {
						ostream.writeObject(state);
						ostream.flush();
						LOG.info("Sent state from persistent server -> " +
							 osock.getRemoteSocketAddress().toString());
						ostream.close();
					}

					osock.close();
				}
				catch(IOException e) {
					LOG.error("Failed to send state to recipient from persisten state server");
					continue;
				}
			}
		}
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
			state = ClusterState.getSingleState();
			listener.newState(state);
		}

		for(;;) {
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
