/**
 * Copyright © 2014
 * Hadoop Replicating Filesystem Cluster Client Interface
 *
 * Client interface for a cluster member, interacts with a 
 * cluster agent to maintain and join the cluster state.
 */
package edu.rit.cs;

public interface ClusterClient
{
	/**
	 * Host address used for TCP data connections, this will
	 * be passed to other members of the cluster.
	 * @return String Host IP address in string form
	 */
	public String getHostAddress();

	/**
	 * Get host port used for TCP data connections, as with the
	 * address, this will be passed to the other cluster members.
	 */
	public int getHostPort();
}
