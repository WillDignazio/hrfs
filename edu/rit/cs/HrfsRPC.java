/**
 * Copyright © 2014
 * Hadoop Replicating Filesystem RPC
 */
package edu.rit.cs;

import java.util.ArrayList;
import java.net.InetSocketAddress;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.ipc.ProtocolInfo;

@InterfaceAudience.Private
@InterfaceStability.Evolving
@ProtocolInfo(protocolName = "hrfs", protocolVersion = 1)
public interface HrfsRPC
{
	/**
	 * Get a list of the peers in the node network.
	 * XXX Probably will get removed.
	 */
	ArrayList<InetSocketAddress> getPeers();
	
	/**
	 * Simple ping->pong response that acknowledges the 
	 * participating node is still active.
	 *
	 * The server is expected to respond with a string
	 * that is non-null.
	 * @return Pong from node target
	 */
	String ping();

	/**
	 * Returns the work queue length of the node, this
	 * is the active number of operations currently
	 * in line to be scheduled.
	 * @return wqlen Length of work queue
	 */
	int wqlen();

	/**
	 * Puts a block into a participating node. The idea
	 * is to abstractly let a node deal with the block
	 * placement. In return, it should give a string key
	 * that was used to store the block.
	 * @param block Block data to store on node
	 */
	String putBlock(byte[] block);

	/**
	 * Gets a block from a participating node. The String
	 * key given is ideally the same kind used to put the
	 * block in the first place.
	 * @param key Remote key for block to retrieve
	 * @return Block data associated with key
	 */
	byte[] getBlock(String key);

	/**
	 * Remove block from a node, this causes the deletion
	 * of a block if the key exists. If the block existed
	 * and was successfully deleted, true is returned.
	 * @param key Key of block to delete on node
	 * @return Whether delete was successful
	 */
	boolean delBlock(String key);
}
