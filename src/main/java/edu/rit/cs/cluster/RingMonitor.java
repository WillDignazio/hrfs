/**
 * Copyright © 2014
 * Hadoop Replicating Filesystem Ring Monitor
 *
 * This class watches over the ring state in the cluster, and if
 * it detects any changes, informs listeners. This monitor uses the
 * ZooKeeper server and api, which gaurantees atomic operations to
 * the ring state, and consistency among the nodes.
 * 
 * @author Will Dignazio <wdignazio@gmail.com>
 * @file RingMonitor.java
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
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.data.Stat;

import edu.rit.cs.HrfsConfiguration;
import edu.rit.cs.HrfsKeys;
import edu.rit.cs.HrfsRing;

import edu.rit.cs.cluster.ClusterLock;

class RingMonitor
	implements StatCallback, Watcher
{
	static
	{
		HrfsConfiguration.init();
	}

	private static final Log LOG = LogFactory.getLog(RingMonitor.class);
	
	private ZooKeeper zk;
	private ClusterLock lock;
	private HrfsRing ring;
	private RingListener listener;
	private boolean dead;
	private String ringpath;

	/**
	 * Configures the monitor to watch the known shared Ring znode
	 * within the zookeeper server.
	 */
	public RingMonitor(ZooKeeper zk,
			   ClusterLock lock,
			   RingListener listener,
			   String ringpath)
	{
		this.zk = zk;
		this.lock = lock;
		this.listener = listener;
		this.dead = false;
		this.ringpath = ringpath;
		
		/* Start off by doing an immediate check */
		zk.exists(ringpath, true, this, null);
	}

	/**
	 * Returns whether the monitor viewing the znode is alive or not.
	 * @return dead Whether the monitor is alive
	 */
	public boolean isAlive()
	{
		if(dead)
			return true;

		return false;
	}

	/**
	 * Handle when a watch event occurs within zookeeper, this is probably
	 * because someone has joined or dropped from the cluster state.
	 * @param event Given event context
	 */
	@Override
	public void process(WatchedEvent event)
	{
		String zpath;

		zpath = event.getPath();
	 	if(event.getType() == Event.EventType.None) {
			/* The state of the _znode_ has changed */
			switch(event.getState())
			{
			case SyncConnected:
				break;
			case Expired:
				dead = true;
				listener.closedHandler(KeeperException.Code.SessionExpired);
				break;
			}
		}
		else {
			if(zpath != null && zpath.equals(ringpath)) {
				/* Something has changed about our node */
				zk.exists(ringpath, true, this, null);
			}
		}
	}
	
	/**
	 * We'd like to know if the ring state exists, and what state it's in.
	 * Regardless of whether it does or doesn't, we need a safe async
	 * way of knowing.
	 * @param rc Result code for znode
	 * @param path Path to znode
	 * @param ctx Context object
	 * @param stat Znode stat struct
	 */
	@Override
	public void processResult(int rc, String path, Object ctx, Stat stat)
	{
		HrfsRing iring;
		boolean exists;
		byte[] buffer;

		exists = false;
		buffer = null;

		switch(rc)
		{
		case Code.Ok:
			exists = true;
			break;
		case Code.NoNode:
			LOG.info("No znode for " + ringpath);
			exists = false;
			break;
		case Code.SessionExpired:
		case Code.NoAuth:
			LOG.error("Invalid Zookeeper Session");
			dead = true;
			listener.closedHandler(rc);
			return;
		default:
			LOG.error("Unknown return code, retrying...");
			zk.exists(ringpath, true, this, null);
			return;
		}
		
		if(exists) {
			try {
				/* Gather the znode data */
				ObjectInputStream istream;
				
				buffer = zk.getData(ringpath, false, null);
				if(buffer == null)
					return;

				/* We've got our ring data, now compare it */
				istream = new ObjectInputStream(new ByteArrayInputStream(buffer));
				iring = (HrfsRing)istream.readObject();
				LOG.info("Deserialized Cluster Ring");

				/* Make sure we got our object, and it's fresh */
				if(iring != this.ring ||
				   (buffer != null && !iring.equals(this.ring))) {
					this.ring = iring;
					listener.ringUpdateHandler(iring);
				}

				istream.close();
			}
			catch(KeeperException e) {
				LOG.error("Ring Monitor Keeper Exception: " + e.toString());
			}
			catch(InterruptedException e) {
				return;
			}
			catch(ClassNotFoundException e) {
				LOG.error("Unable to read the ring state from zookeeper: " + e.toString());
				return;
			}
			catch(IOException e) {
				LOG.error("Failed to read cluster state change: " + e.toString());
				return;
			}
		}
	}
}
