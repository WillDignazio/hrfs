/**
 * Block Party Filesystem RPC
 *
 */
package edu.rit.cs;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.ipc.ProtocolInfo;

@InterfaceAudience.Private
@InterfaceStability.Evolving
@ProtocolInfo(protocolName = "bpfs", protocolVersion = 1)
public interface BpfsRPC
{
	/**
	 * Simple ping->pong response that acknowledges the 
	 * participating node is still active.
	 *
	 * The server is expected to respond with a string
	 * that is non-null;
	 */
	String ping();

	/**
	 * Returns the work queue length of the node, this
	 * is the active number of operations currently
	 * in line to be scheduled.
	 */
	int wqlen();

	/**
	 * Puts a block into a participating node. The idea
	 * is to abstractly let a node deal with the block
	 * placement. In return, it should give a string key
	 * that was used to store the block.
	 */
	String putBlock(byte[] block);

	/**
	 * Gets a block from a participating node. The String
	 * key given is ideally the same kind used to put the
	 * block in the first place.
	 */
	byte[] getBlock(String key);

	/**
	 * Remove block from a node, this causes the deletion
	 * of a block if the key exists. If the block existed
	 * and was successfully deleted, true is returned.
	 */
	boolean delBlock(String key);
}
