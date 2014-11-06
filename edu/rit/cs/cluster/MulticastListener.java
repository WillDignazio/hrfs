/**
 * Copyright © 2014 Will Dignazio
 * Multicast Listener
 *
 * Interface that allows an object to listen for notifications on
 * the multicast network from the MulticastServer. This includes
 * operations like node insertion and removal.
 */
package edu.rit.cs.cluster;

interface MulticastListener
{
	/**
	 * A node has announced itself on the network, and the implementor
	 * needs to handle things like giving it a copy of the nodes state,
	 * and ring rebalancing.
	 * @param host Host that node is expecting state from
	 * @param port Port that node is expecting state from
	 */
	public void newNode(String host, int port);

	/**
	 * A node wishes to join the cluster, and has derived a new state
	 * from it, we need to fetch that state and do something with it.
	 * @param host Host to receive the new cluster state from.
	 * @param port Port to receive the new cluster state from.
	 */
	public void nodeJoin(String host, int port);
}
