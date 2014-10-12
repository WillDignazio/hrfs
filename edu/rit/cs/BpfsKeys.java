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
	public static final String BPFS_NODE_ADDRESS	= "bpfs.node.address";
	public static final String BPFS_NODE_PORT	= "bpfs.node.port";
	public static final String BPFS_BLKSZ		= "bpfs.blksz";

	public static final String BPFS_DEFAULT_URI_SCHEME	= "bpfs";
}
