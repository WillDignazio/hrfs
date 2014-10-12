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
	String ping();
}
