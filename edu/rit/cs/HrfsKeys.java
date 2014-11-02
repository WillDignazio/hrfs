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
	public static final String HRFS_NODE_ADDRESS	= "hrfs.node.address";
	public static final String HRFS_NODE_PATH	= "hrfs.node.path";
	public static final String HRFS_NODE_PORT	= "hrfs.node.port";
	public static final String HRFS_NODE_GROUP_ADDRESS	= "hrfs.node.group.address";
	public static final String HRFS_NODE_GROUP_PORT		= "hrfs.node.group.port";

	public static final String HRFS_BLKSZ		= "hrfs.blksz";

	/* Assinged Value Constants */
	public static final String HRFS_DEFAULT_URI_SCHEME	= "hrfs";
}
