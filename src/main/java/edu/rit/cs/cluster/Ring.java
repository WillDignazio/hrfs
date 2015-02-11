/**
 * Copyright © 2014
 * Hrfs Ring Object and Utilities
 *
 * @file Ring.java
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

public final class Ring<H extends HashCode>
	implements Serializable
{

	private final SortedMap<H, RingNode> ring;
       
	private transient HashFunction hashFunction;
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

		/* Shield in default constructor */
		private RingNode() { }
		
		RingNode(H hash, InetSocketAddress addr)
		{
			this.hash = hash;
			this.address = addr;
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
			return address.getPort();
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

	/* Hide the default constructor */
	public Ring(HashFunction hfn)
	{
		this.hashFunction = hfn;
		this.ring = new TreeMap<H, RingNode>();
		this.conf = new HrfsConfiguration();
	}

	public Ring(HashFunction hashFunction,
			Collection<RingNode> nodes)
	{
		this.ring = new TreeMap<H, RingNode>();
		this.hashFunction = hashFunction;
		this.conf = new HrfsConfiguration();

		for(RingNode node : nodes)
			add(node);
	}

	public RingNode createNode(H hash, InetSocketAddress addr)
	{
		return new RingNode(hash, addr);
	}

	public Ring add(RingNode node)
	{
		Ring nring;
		Collection<RingNode> nodes;

		nodes = new LinkedList<RingNode>();
		for(RingNode prevnode : ring.values())
			nodes.add(prevnode);

		nring = new Ring(hashFunction, nodes);
		return nring;
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
