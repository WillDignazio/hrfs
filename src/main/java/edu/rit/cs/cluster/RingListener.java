/**
 * Copyright © 2014
 * Hadoop Replicating Filesystem Ring Event Listener
 *
 * After a monitor has received a notification about the change in
 * the hrfs state znode, it will call to one of these methods.
 * Listening objects may then perform any necessary response action,
 * such as rebalancing block data or setting their view of the new node
 * ring.
 * 
 * @author Will Dignazio <wdignazio@gmail.com>
 * @file RingListener.java
 */
package edu.rit.cs.cluster;

import edu.rit.cs.HrfsRing;

public interface RingListener
{
	/**
	 * The state of the ring object has changed, the listener
	 * needs to act accordingly. The given object is the
	 * state of the ring after the change.
	 * @param ring HrfsRing after change
	 */
	void ringUpdateHandler(HrfsRing ring);

	/**
	 * The session that was maintained with ZooKeeper is no longer
	 * valid, and must be gracefully closed.
	 * @param rc Reason session is invalid
	 */
	void closedHandler(int rc);
}
