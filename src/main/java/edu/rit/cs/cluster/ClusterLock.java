/**
 * Copyright © 2014
 * Hadoop Replicating Filesystem Cluster Lock
 *
 * This lock is a cluster wide mutex that can be used to enforce
 * ordering of events. This ensures that a node in the cluster
 * holding a lock will keep other nodes at bay while blocking on it.
 *
 * @author Will Dignazio <wdignazio@gmail.com>
 * @file ClusterLock.java
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

import org.apache.zookeeper.*;
import org.apache.zookeeper.ZooDefs.*;

import edu.rit.cs.HrfsConfiguration;
import edu.rit.cs.HrfsRing;

class ClusterLock
{
	static
	{
		HrfsConfiguration.init();
	}

	private static final Log LOG = LogFactory.getLog(ClusterLock.class);
	private static final String LOCKBASE = "/";

	private final ZooKeeper zk;
	private String lockPath;
	private String lid;

	public ClusterLock(ZooKeeper zk, String lid)
	{
		this.lockPath = null;
		this.zk = zk;
		this.lid = lid;
	}

	/**
	 * Lock this cluster lock, all other cluster nodes attempting to 
	 * lock on this lock will block until it has been unlocked.
	 * This is a networked mutex.
	 */
	public void lock()
		throws IOException
	{
		try {
			/* Open up a znode on a shared path */
			lockPath = zk.create(LOCKBASE + lid,
					     null, /* Node Data */
					     Ids.OPEN_ACL_UNSAFE,
					     CreateMode.EPHEMERAL_SEQUENTIAL);

			final Object lock = new Object();

			synchronized(lock) {
				while(true) {
					/* We're getting the nodes watching the shared lock */
					List<String> nodes = zk.getChildren(LOCKBASE, new Watcher() {
							@Override
							public void process(WatchedEvent event) {
								synchronized(lock) {
									lock.notifyAll();
								}
							}
						});

					/*
					 * Order for coherence, we'll get a "larger" id
					 * we return for another lock.
					 */
					Collections.sort(nodes);
					if(this.lockPath.endsWith(nodes.get(0)))
						return; // We're the holder
					else
						lock.wait();
				}
			}
		}
		catch(KeeperException e) {
			LOG.error("Keeper Exception: " + e.toString());
			throw new IOException(e);
		}
		catch(InterruptedException e) {
			LOG.error("Interrupted Exception: " + e.toString());
			throw new IOException(e);
		}
	}

	public void unlock()
		throws IOException
	{
		try {
			/* Give up our lock */
			zk.delete(lockPath, -1);
			lockPath = null;
		}
		catch(KeeperException e) {
			LOG.error("Keeper Exception: " + e.toString());
			throw new IOException(e);
		}
		catch(InterruptedException e) {
			LOG.error("Interrupted Exception: " + e.toString());
			throw new IOException(e);
		}
	}
}
