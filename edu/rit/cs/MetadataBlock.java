/**
 * Copyright © 2014
 * Metadata Block Object
 *
 * @file MetadataBlock.java
 * @author Will Dignazio <wdignazio@gmail.com>
 */
package edu.rit.cs;

import org.apache.hadoop.classification.InterfaceStability;
import java.io.Serializable;
import java.util.List;

@InterfaceStability.Evolving
public class MetadataBlock
	implements Serializable
{
	public static int	METATYPE_NULL	= 0;
	public static int	METATYPE_DIR	= 1;
	public static int	METATYPE_FILE	= 2;

	private String path;		// Full file path
	private long nblocks;		// Number of blocks in file
	private long size;		// Size of the file in bytes
	private int type;		// Type of metadata represents

	private int uid;		// User ID
	private int gid;		// Group ID
	private short perms;		// Permissions

	private List<String> blk_keys;	// List of block keys

	/**
	 * Initialize an empty metadata block, contains no data and will
	 * have values initialized to 0.
	 */
	public MetadataBlock()
	{
		this.path = null;
		this.nblocks = 0;
		this.size = 0;
		this.type = METATYPE_NULL;
		this.uid = 0;
		this.gid = 0;
		this.perms = 0;
		this.blk_keys = null;
	}

	/**
	 * Create a new Metadata block that has all the necessary properties
	 * to be written to the cluster.
	 * @param path		Full pathname within cluster
	 * @param nblocks	Number of blocks in file
	 * @param size		Size of the file in bytes
	 * @param type		Type of file
	 * @param uid		UID of file
	 * @param gid		GID of file
	 * @param perms		Permissions of file
	 * @param keys		Block keys
	 */
	public MetadataBlock(String path,
			     long nblocks,
			     long size,
			     int type,
			     int uid,
			     int gid,
			     short perms,
			     List<String> blk_keys)
	{
		this.path = path;
		this.nblocks = nblocks;
		this.size = size;
		this.type = type;
		this.uid = uid;
		this.gid = gid;
		this.perms = perms;
		this.blk_keys = blk_keys;
	}
}
