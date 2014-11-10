/**
 * Copyright © 2014
 * Hadoop Replicating Filesystem Cluster Agent
 * 
 * Representative to a multicast cluster group, allows
 * for a group of nodes to make placements and ring adjustments
 * on behalf of a node.
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
import org.apache.commons.lang.SerializationUtils;

import org.apache.zookeeper.*;
import org.apache.zookeeper.ZooDefs.*;

import edu.rit.cs.HrfsConfiguration;
import edu.rit.cs.HrfsKeys;
import edu.rit.cs.HrfsRing;

public class ClusterAgent
	extends Thread
	implements RingListener
{
	static
	{
		HrfsConfiguration.init();
	}

	private static final String RING_ZNODE_PATH = "/hrfs-ring";
	private static final Log LOG = LogFactory.getLog(ClusterAgent.class);

	private InetSocketAddress rpcAddr;
	private HrfsConfiguration conf;
	private HrfsRing ring;
	private ClusterClient client;
	private ClusterLock lock;
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
	public ClusterAgent(ClusterClient client)
		throws IOException
	{
		this.conf = new HrfsConfiguration();
		this.client = client;

		/* Setup the ZooKeeper session */
		this.zk = new ZooKeeper(
			conf.get(HrfsKeys.HRFS_ZOOKEEPER_ADDRESS, "127.0.0.1"),
			conf.getInt(HrfsKeys.HRFS_ZOOKEEPER_PORT, 2181),
			new AgentWatcher());

		/* For ring addition */
		this.rpcAddr = new InetSocketAddress(client.getRPCAddress(),
						     client.getRPCPort());
		this.lock = new ClusterLock(zk);
		this.monitor = new RingMonitor(zk, lock, this, RING_ZNODE_PATH);
	}

	/** Handler for ring state changes */
	@Override
	public synchronized void ringUpdate(HrfsRing ring)
	{
		byte[] buffer;
		HrfsRing nring;

		synchronized(this) {
			this.ring = ring;

			try {
				/* Just for a moment */
				lock.lock();
			
				if(!this.ring.getHosts().contains(this.rpcAddr)) {
					LOG.info("Node not present in ring, attempting to join....");
					
					/*
					 * We are going to need to generate a new ring with us in
					 * it, then publish that ring. We have the lock here, so
					 * the cluster ring shouldn't change on us.
					 */
					try {
						nring = HrfsRing.generateRing(ring, this.rpcAddr);
						buffer = SerializationUtils.serialize(nring);
						zk.setData(RING_ZNODE_PATH, buffer, -1);
					}
					catch(KeeperException e) {
						LOG.error("Unable to join node: " +
							  e.toString());
					}
					catch(InterruptedException e) {
						LOG.error("Interrupted while joining node" +
							  e.toString());
					}
				}

				lock.unlock();
			}
			catch(IOException e) {
				LOG.error("Failed to update ring with us in it.");
			}
		}
	}

	/** Handler for ring destruction, or invalid zookeeper session */
	@Override
	public synchronized void closed(int rc)
	{
		LOG.error("ZooKeeper session invalid");
		notifyAll();
	}

	/** Handler for needing a new ring state */
	@Override
	public void newRing(HrfsRing ring)
	{
		HrfsRing newring;
		byte buffer[];
		
		LOG.warn("Cluster ring not initialized.");

		/* Blank new ring (including us) */
		newring = new HrfsRing();
		
		/*
		 * At this point, we're the first here, and we need to
		 * create a new ring that represents just us in the cluster.
		 * We need the cluster lock in case anybody else comes up.
		 * We also need to watch for getting the lock a hair too late,
		 * perhaps right after another node tried to create the 
		 * cluster.
		 */
		try {
			lock.lock();

			/* Create a new HrfsRing */
			buffer = SerializationUtils.serialize(newring);

			zk.create(RING_ZNODE_PATH,
				  buffer,
				  Ids.OPEN_ACL_UNSAFE,
				  CreateMode.PERSISTENT);

			LOG.info("Created new ring state");
			lock.unlock();
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
}
