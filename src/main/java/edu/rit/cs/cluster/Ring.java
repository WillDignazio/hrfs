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
import com.google.common.hash.Hashing;

import edu.rit.cs.HrfsKeys;
import edu.rit.cs.HrfsConfiguration;

public final class Ring<H extends HashCode>
	implements Serializable
{
	public static final String HASH_UNSET	= "UNSET";
	public static final String HASH_SHA1	= "SHA1";
	
	private final SortedMap<H, RingNode> ring;
	private final String hashFunctionString;
       
	private transient HashFunction _hashFunction;
	private transient HrfsConfiguration _conf;

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

	/**
	 * Translates a hash function from the string saved by the transient
	 * property of the ring. This is necessary as the serialization nukes
	 * the HashFunction for the object.
	 */
	private HashFunction translateFromHashString(String hstr)
	{
		switch(hstr)
		{
		case HASH_UNSET:
			return null;
		case HASH_SHA1:
			return Hashing.sha1();
		default:
			return null;
		}
	}
	
	/**
	 * Get immutable hash function string for this Ring, this is also nifty
	 * as we can't serialize the google HashFunction, which must be set by
	 * the ClusterManager.
	 */
	public HashFunction getHashFunction()
	{
		/* Transient property may not be set */
		if(_hashFunction != null)
			return _hashFunction;		
		else if(hashFunctionString.equals(HASH_UNSET))
			return null;
		else
			return translateFromHashString(hashFunctionString);
	}
	
	/**
	 * Build the Ring with the default hash function, this will as of this
	 * revision be the SHA1 algorithm.
	 */
	public Ring()
	{
		this.hashFunctionString = HASH_SHA1;
		this.ring = new TreeMap<H, RingNode>();
		this._hashFunction = translateFromHashString(hashFunctionString);
		this._conf = new HrfsConfiguration();
	}

	public Ring(String hashstr)
	{
		this.hashFunctionString = hashstr;
		this.ring = new TreeMap<H, RingNode>();
		this._hashFunction = translateFromHashString(hashFunctionString);
		this._conf = new HrfsConfiguration();
	}

	public Ring(String hashstr, Collection<RingNode> nodes)
	{
		this.ring = new TreeMap<H, RingNode>();
		this.hashFunctionString = hashstr;
		this._hashFunction = translateFromHashString(hashstr);
		this._conf = new HrfsConfiguration();

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

		nring = new Ring(hashFunctionString, nodes);
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
