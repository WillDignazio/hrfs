/**
 * Copyright © 2014
 * Hadoop Replicating Filesystem Cluster Agent
 * 
 * Representative to a multicast cluster group, allows
 * for a group of nodes to make placements and ring adjustments
 * on behalf of a node.
 *
 * @author Will Dignazio <wdignazio@gmail.com>
 * @file RingManager.java
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
import org.apache.commons.lang.SerializationUtils;
import org.apache.zookeeper.*;
import org.apache.zookeeper.ZooDefs.*;

import com.google.common.base.Charsets;
import com.google.common.hash.Hashing;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;

import edu.rit.cs.HrfsConfiguration;
import edu.rit.cs.HrfsKeys;
import edu.rit.cs.Ring;

public class RingManager
	extends Thread
	implements RingListener
{
	static
	{
		HrfsConfiguration.init();
	}

	private static final String CURRENT_RING_LOCK = "ringlock";
	private static final String RING_ZNODE_PATH = "/hrfs-ring";
	private static final Log LOG = LogFactory.getLog(RingManager.class);


	private InetSocketAddress node_addr;
	private HashCode chash;
	private HrfsConfiguration conf;
	private Ring.RingNode manager_rnode;
	private ClusterLock ringlock;
	private RingMonitor monitor;
	private ZooKeeper zk;

	/** Helper that processes events to the monitor */
	private class AgentWatcher
		implements Watcher {
		@Override
		public void process(WatchedEvent event) {
			monitor.process(event);
		}
	}

	/**
	 * Sets up our connection to the ZooKeeper group, and initiates
	 * the components that are active in it. This includes the shared
	 * cluster lock, and the watcher for the cluster ring state.
	 */
	public RingManager(InetSocketAddress node_addr)
		throws IOException
	{
		HashFunction hf;
		String suuid;		
		File fuuid;
		Ring ring;

		suuid = null;
		hf = Hashing.sha1();
		this.conf = new HrfsConfiguration();

		/* Initialize the agent to a static hash value */
		fuuid = new File(conf.get(HrfsKeys.HRFS_NODE_PATH, null) + "/uuid");
		if(fuuid.exists()) {
			BufferedReader reader;
			reader = new BufferedReader(new FileReader(fuuid));
			suuid = reader.readLine();
			reader.close();

		}

		if(suuid == null || suuid.equals("")) {
			BufferedWriter writer;
			writer = new BufferedWriter(new FileWriter(fuuid));
			suuid = UUID.randomUUID().toString();
			LOG.info("Generated new agent UUID: " + suuid);
			
			writer.write(suuid);
			writer.flush();
			writer.close();
		}

		LOG.info("Builing uuid chash from " + suuid);
		chash = hf.newHasher()
			.putString(suuid, Charsets.UTF_8)
			.hash();		
		LOG.info("Cluster agent chash: " + chash.toString());

		/* Setup the ZooKeeper session */
		this.zk = new ZooKeeper(
			conf.get(HrfsKeys.HRFS_ZOOKEEPER_ADDRESS, "127.0.0.1"),
			conf.getInt(HrfsKeys.HRFS_ZOOKEEPER_PORT, 2181),
			new AgentWatcher());		
		
		/* Recogize the global cluster lock for all managers */
		this.ringlock = new ClusterLock(zk, CURRENT_RING_LOCK);
		this.monitor = new RingMonitor(zk, ringlock, this, RING_ZNODE_PATH);

		/* We're going to check if the ring exists, create if not */
		ring = getRing();
		if(ring == null)
			ring = createRing();

		/*
		 * Regardless, create a local node representation for the
		 * manager of the node.
		 */
		this.manager_rnode = ring.createNode(chash, node_addr);	       		
	}

	/** Handler for ring state changes */
	@Override
	public synchronized void ringUpdateHandler(Ring ring)
	{
		byte[] buffer;
		Ring nring;


		LOG.info("Hit Ring Update Handler");
		/* XXX TODO Handle updates to ring */
	}

	/** Handler for ring destruction, or invalid zookeeper session */
	@Override
	public synchronized void closedHandler(int rc)
	{
		LOG.error("ZooKeeper session invalid");
		notifyAll();
	}

	/**
	 * Sets the ring within the cluster, helper method to be used with
	 * appropriate locking by the manager. Will cause Zookeeper to notify
	 * all of the listening nodes that the Ring has been changed.
	 */
	private void setRing(Ring ring)
		throws IOException
	{
		byte ringbuf[];
		
		try {
			LOG.info("Serializing ring for cluster...");
			ringbuf = SerializationUtils.serialize(ring);
			zk.create(RING_ZNODE_PATH,
				  ringbuf,
				  Ids.OPEN_ACL_UNSAFE,
				  CreateMode.PERSISTENT);
			LOG.info("Created new ring state");
		}
 		catch(InterruptedException e) {
		 	LOG.error("Interrupted while creating ring " + e.toString());
			throw new IOException(e.getMessage());
		}
		catch(KeeperException e) {
			LOG.error("Keeper Failure: Failed to create new ring: " +
				  e.toString());
			throw new IOException(e.getMessage());
		}
	}
	
	public Ring createRing()
		throws IOException
	{
		Ring ring;
		Ring.RingNode orig;
		LinkedList<Ring.RingNode> nodes;

		ringlock.lock();
		if(getRing() != null) {
			LOG.warn("Race to create new node, this node lost");
			ringlock.unlock();
			return getRing();
		}

		/* Create an empty ring of SHA1 */
		ring = new Ring(Hashing.sha1());

		nodes = new LinkedList<Ring.RingNode>();
		nodes.add(manager_rnode);

		/* Create the brand new ring. */
		ring = new Ring(Hashing.sha1(), nodes);
		setRing(ring);

		ringlock.unlock();
		return ring;
	}

	/** 
	 * Attempt to join the ring of the cluster. If the node is already in
	 * cluster, than this will simply return without doing anything. If the
	 * node is not in cluster, then the manager will attempt to start the
	 * process of moving the ring to a new state.
	 */
	public Ring joinRing()
	{
		Ring ring;

		ring = null;
		try {
			if(getRing() == null) {
				LOG.warn("Attempting to join null cluster ring");
				/* Callback will set our manager ring */
				ring = createRing();
			}

			ringlock.lock();

			ringlock.unlock();
		}
		catch(IOException e) {
			LOG.error("Failed to create new ring: " +
				  e.toString());
			System.exit(1);
		}

		return ring;
	}

	/**
	 * Get the ring of the cluster.
	 * @return ring Cluster ring
	 */
	public Ring getRing()
		throws IOException
	{
		ByteArrayInputStream istream;
		ObjectInput in;
		byte[] ringbuf;
		Ring ring;

		ring = null;

		try {
			/* Obviously, there is no ring yet */
			if(zk.exists(RING_ZNODE_PATH, null) == null)
				return null;
			
			ringbuf = zk.getData(RING_ZNODE_PATH, false, null);
			istream = new ByteArrayInputStream(ringbuf);
			in = new ObjectInputStream(istream);
			ring = (Ring)in.readObject();
		}
		catch(ClassNotFoundException e) {
			LOG.error("Could not find Ring object class template");
			System.exit(1);
		}
		catch(KeeperException e) {
			LOG.error("Keeper failed to retrieve current ring.");
			throw new IOException(e.getMessage());
		}
		catch(InterruptedException e) {
			LOG.error("Interrupted while retrieving ring.");
			throw new IOException(e.getMessage());
		}
			
		return ring;
	}
}
