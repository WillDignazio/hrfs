/**
 * Copyright © 2014
 * Hadoop Replicating Filesystem Cluster State
 *
 * State object that is shared amongst members of the hrfs cluster.
 */
package edu.rit.cs.cluster;

import java.io.*;
import java.net.*;
import java.util.*;

import edu.rit.cs.HrfsRing;

public class ClusterState
	implements Serializable, Comparable<ClusterState>
{
	private int nactive;		// # of known active noes
	private int ndead;		// # of known dead nodes
	private long timestamp;		// Monotonic timestamp

	/**
	 * ClusterState objects are also immutable, so they must
	 * be constructed with all fields initalized.
	 */
	public ClusterState(int nactive, int ndead)
	{
		this.nactive = nactive;
		this.ndead = ndead;
		this.timestamp = System.currentTimeMillis();
	}

	/**
	 * Considered a "blank state", used for single node instances
	 * that are just creating/joining a cluster.
	 */
	public ClusterState()
	{
		this.nactive = 1;
		this.ndead = 0;
		this.timestamp = 0; // For immediate revision
	}

	/**
	 * Utility method that generates a valid single node state.
	 * This is useful for when a new cluster is created, and the
	 * participating node doesn't have any state.
	 * @return Single node state
	 */
	public static ClusterState getSingleState()
	{
		return new ClusterState();
	}

	/**
	 * Override of the compareTo for timestamp comparison, this compares
	 * to Cluster States for priority.
	 * @param state State to compare against
	 */
	@Override
	public int compareTo(ClusterState state)
	{
		if(this.timestamp > state.getTimestamp())
			return 1;
		else if(this.getTimestamp() < state.getTimestamp())
			return -1;

		return 0;
	}

	/**
	 * Returns the number of known active nodes.
	 * @return int Number of active Nodes
	 */
	public int getNodesActive()
	{
		return this.nactive;
	}

	/**
	 * Returns the number of known dead nodes.
	 * @return int Number of dead Nodes
	 */
	public int getNodesDead()
	{
		return this.ndead;
	}

	/**
	 * Return the timestamp for the state.
	 * @return long Timestamp for the state
	 */
	public long getTimestamp()
	{
		return this.timestamp;
	}
}