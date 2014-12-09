/**
 * Copyright © 2014
 * Hadoop Replicating Filesystem Cluster Client Interface
 * 
 * Client interface that allows the cluster to gather some
 * basic information from the HrfsNode. This will be used
 * to inform the other nodes of the RPC port and address
 * that each node is accessible with.
 *
 * @author Will Dignazio <wdignazio@gmail.com>
 * @file ClusterAgent.java
 */
package edu.rit.cs.cluster;

public interface ClusterClient
{
	/**
	 * Get the RPC port of the Node.
	 * @return port RPC port of the node.
	 */
	public int getRPCPort();

	/**
	 * Get the RPC server address of the node.
	 * @return address RPC address of the node.
	 */
	public String getRPCAddress();
}
