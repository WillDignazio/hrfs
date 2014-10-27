/**
 * Copyright © 2014
 * Hadoop Replicating Filesystem Keys File
 */
package edu.rit.cs;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.CommonConfigurationKeys;

@InterfaceAudience.Private
public final class HrfsKeys
	extends CommonConfigurationKeys
{
	/* Node Configuration Keys */
	public static final String HRFS_CLIENT_NODES	= "hrfs.data.nodes";
	public static final String HRFS_NODE_ADDRESS	= "hrfs.node.address";
	public static final String HRFS_NODE_PATH	= "hrfs.node.path";
	public static final String HRFS_NODE_PORT	= "hrfs.node.port";

	/* Global State Configuration Keys */
	public static final String HRFS_BLKSZ		= "hrfs.blksz";

	/* Assinged Value Constants */
	public static final String HRFS_DEFAULT_URI_SCHEME	= "hrfs";
}
