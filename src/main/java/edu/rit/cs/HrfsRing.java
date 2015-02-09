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
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.SortedMap;
import java.util.TreeMap;

import com.google.common.hash.HashFunction;
import com.google.common.hash.HashCode;

import edu.rit.cs.HrfsKeys;
import edu.rit.cs.HrfsConfiguration;

public final class HrfsRing<H extends HashCode>
	implements Serializable
{
	private final HashFunction hashFunction;
	private final SortedMap<H, RingNode> ring;

	private transient HrfsConfiguration conf;

	/**
	 * Node that composes the hash ring of the cluster. The node contains the
	 * location from which the ring member can be reached,and the port that
	 * it will respond from.
	 * Each node has an assigned, static, hash field that will be used to
	 * figure out where incoming hash values will go via the get() method.
	 */
	public class RingNode
		implements Serializable
	{
		private H hash;
		private InetSocketAddress address;
		private int port;

		public RingNode(H hash, InetSocketAddress addr, int port)
		{
			this.hash = hash;
			this.address = addr;
			this.port = port;
		}

		/**
		 * Gets the hash value assigned to this node.
		 * @return hash Hash value for the node.
		 */
		public H getHash()
		{
			return hash;
		}

		/**
		 * Gets the port associated with this ring member node.
		 * @return port Port for the ring member will respond to.
		 */
		public int getPort()
		{
			return port;
		}

		/**
		 * Gets the address associate with this ring member node.
		 * @return address Address for this node.
		 */
		public InetSocketAddress getAddress()
		{
			return address;
		}
	}

	public HrfsRing(HashFunction hashFunction,
			Collection<RingNode> nodes)
	{
		this.ring = new TreeMap<H, RingNode>();
		this.hashFunction = hashFunction;
		this.conf = new HrfsConfiguration();

		for(RingNode node : nodes)
			add(node);
	}

	public void add(RingNode node)
	{
		ring.put(node.getHash(), node);
	}

	public boolean contains(RingNode node)
	{
		H hash;
		
		hash = node.getHash();
		if(ring.get(hash) != null)
			return true;

		return false;
	}

	public void remove(RingNode node)
	{
		ring.remove(node.getHash());
	}

	public RingNode get(H hash)
	{
		if(ring.isEmpty())
			return null;

		if(!ring.containsKey(hash)) {
			SortedMap<H, RingNode> tailMap;

			/* Returns a list greater than this hash */
			tailMap = ring.tailMap(hash);

			/* Grab the first of the list */
			hash = tailMap.isEmpty() ? ring.firstKey() : tailMap.firstKey();
		}

		return ring.get(hash);
	}
}
