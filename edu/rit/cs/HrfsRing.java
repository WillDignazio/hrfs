/**
 * Copyright © 2014
 * Hrfs Ring Object and Utilities
 *
 * @file HrfsRing.java
 * @author Will Dignazio <wdignazio@gmail.com>
 */
package edu.rit.cs;

import java.util.*;
import java.io.Serializable;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

import edu.rit.cs.HrfsKeys;
import edu.rit.cs.HrfsConfiguration;

public final class HrfsRing
	implements Serializable
{
	/*
	 * XXX Incomplete
	 *
	 * For now we're just going to leave this as a list of nodes
	 * that all blocks will map to. This is obviously wrong as we
	 * want to have a consistent hash ring that blocks will only
	 * partially map to.
	 */
	private ArrayList<String> hosts;
	private transient HrfsConfiguration conf;

	/**
	 * Construct a new HrfsRing without any prior knowledge of
	 * other Hrfs Nodes, this is akin to forming a new
	 * cluster, or new filesystem.
	 * @param host Host address of [first] node
	 */
	public HrfsRing()
	{
		conf = new HrfsConfiguration();
		hosts = new ArrayList<String>();
		hosts.add(conf.get(HrfsKeys.HRFS_NODE_ADDRESS, "127.0.0.1"));
	}

	/**
	 * Constructor that creates a new ring based on a list known hosts.
	 */
	private HrfsRing(ArrayList<String> hosts, String newhost)
	{
		this.hosts = hosts;
		conf = new HrfsConfiguration();
		this.hosts = new ArrayList<String>(hosts);
		this.hosts.add(newhost);
	}

	/**
	 * Map a key to a given set of hosts, return them as a list.
	 * @param key Hash key
	 */
	public List<String> mapKey(String key)
	{
		/* XXX Read header note */
		return hosts;
	}

	public ArrayList<String> getHosts()
	{
		return this.hosts;
	}

	/**
	 * Create a new immutable Ring that contains the given host.
	 */
	public static HrfsRing generateRing(HrfsRing ring, String host)
	{
		return new HrfsRing(ring.getHosts(), host);
	}
}
