/**
 * Copyright © 2014
 * State Listener
 *
 * Interface that allows one to listen for changes in cluster state
 * from a State Server. By implementing these methods, one can
 * receive new states from another node.
 *
 * @author Will Dignazio <wdignazio@gmail.com>
 * @file StateListener.java
 */
package edu.rit.cs.cluster;

import edu.rit.cs.cluster.ClusterState;

interface StateListener
{
	/**
	 * A new state has been issued, and it is provided by the argument
	 * to this interface. This state is safe to reference, and may be
	 * used by the listening implementor.
	 * @param state New Cluster State
	 */
	public void newState(ClusterState state);
}
