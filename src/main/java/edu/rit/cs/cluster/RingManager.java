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
import edu.rit.cs.HrfsRing;

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

	private HashCode chash;
	private HrfsConfiguration conf;
	private HrfsRing ring;
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
	public RingManager()
		throws IOException
	{
		HashFunction hf;
		String suuid;		
		File fuuid;

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

		/* Recogize the global cluster lock for all agents */
		this.ringlock = new ClusterLock(zk, CURRENT_RING_LOCK);
		this.monitor = new RingMonitor(zk, ringlock, this, RING_ZNODE_PATH);
	}

	/** Handler for ring state changes */
	@Override
	public synchronized void ringUpdateHandler(HrfsRing ring)
	{
		byte[] buffer;
		HrfsRing nring;

		this.ring = ring;

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
	 * Set the ring of the cluster.
	 * @param ring The new ring.
	 */
	public void setRing(HrfsRing ring)
	{
		byte buffer[];
		
		LOG.warn("Cluster ring not initialized.");
		
		/*
		 * At this point, we're the first here, and we need to
		 * create a new ring that represents just us in the cluster.
		 * We need the cluster lock in case anybody else comes up.
		 * We also need to watch for getting the lock a hair too late,
		 * perhaps right after another node tried to create the 
		 * cluster.
		 */
		try {
			ringlock.lock();

			buffer = SerializationUtils.serialize(ring);
			zk.create(RING_ZNODE_PATH,
				  buffer,
				  Ids.OPEN_ACL_UNSAFE,
				  CreateMode.PERSISTENT);

			LOG.info("Created new ring state");
			ringlock.unlock();
		}
		catch(InterruptedException e) {
			LOG.error("Interrupted while creating ring " + e.toString());
			System.exit(1);
		}
		catch(KeeperException e) {
			LOG.error("Keeper Failure: Failed to create new ring: " +
				  e.toString());
			System.exit(1);
		}
		catch(IOException e) {
			LOG.error("Failed to create new ring: " +
				  e.toString());
			System.exit(1);
		}
	}

	/**
	 * Get the ring of the cluster.
	 * @return ring Cluster ring
	 */
	public HrfsRing getRing()
	{
		return this.ring;
	}
}
