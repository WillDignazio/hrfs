/**
 * Block Party Filesystem Keys File
 *
 */
package edu.rit.cs;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.CommonConfigurationKeys;

@InterfaceAudience.Private
public final class BpfsKeys
	extends CommonConfigurationKeys
{
	/* Node Configuration Keys */
	public static final String BPFS_CLIENT_NODES	= "bpfs.data.nodes";
	public static final String BPFS_NODE_ADDRESS	= "bpfs.node.address";
	public static final String BPFS_NODE_PATH	= "bpfs.node.path";
	public static final String BPFS_NODE_PORT	= "bpfs.node.port";

	/* Global State Configuration Keys */
	public static final String BPFS_BLKSZ		= "bpfs.blksz";

	/* Assinged Value Constants */
	public static final String BPFS_DEFAULT_URI_SCHEME	= "bpfs";
}
