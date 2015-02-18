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
	public static final String	HRFS_NODE_ADDRESS	= "hrfs.node.address";
	public static final String	HRFS_NODE_PATH		= "hrfs.node.path";
	public static final String 	HRFS_NODE_PORT		= "hrfs.node.port";
	public static final String	HRFS_NODE_STORE_PATH	= "hrfs.node.store.path";
	
	public static final String HRFS_ZOOKEEPER_ADDRESS	= "hrfs.zookeeper.address";
	public static final String HRFS_ZOOKEEPER_PORT		= "hrfs.zookeeper.port";

	/* Assinged Value Constants */
	public static final String HRFS_DEFAULT_URI_SCHEME	= "hrfs";
}
