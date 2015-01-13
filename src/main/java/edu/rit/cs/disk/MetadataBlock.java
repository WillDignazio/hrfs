/**
 * Copyright © 2015
 * Hadoop Replicating File System
 *
 * Smallest metadata object, a block within a metadata extent, is
 * an in memory representation of the mapped memory on disk. This
 * is used to provide an interface to metadata values, and information
 * about where data lies.
 */
package edu.rit.cs.disk;

import java.util.Arrays;
import java.nio.ByteBuffer;
import java.nio.ByteBuffer;

class MetadataBlock
{
	private ByteBuffer mbuf;
	private int mxn;
	private int offset;

	/**
	 * In memory object for metadata block of mapped region, when
	 * given the number within the extent, this will calculate the
	 * offsets for the set and get methods.
	 * @param mbuf Parent byte buffer
	 * @param mxn Metadata block number
	 */
	public MetadataBlock(ByteBuffer mbuf, int mxn)
	{
		this.mbuf = mbuf;
		this.mxn = mxn;
		this.offset = mxn * HrfsDisk.METADATA_BLOCK_SIZE;
	}

	/**
	 * Get the key for the metadata block, this is used to navigate
	 * the tree, and determine the location of a block of data.
	 * 
	 * This returns a *copy* of the value found on disk from this node.
	 * @return key Key of this metadata block
	 */
	public byte[] getKey()
	{
		byte[] cregion;

		cregion = Arrays.copyOfRange(this.mbuf.array(),
					     offset,
					     offset +  HrfsDisk.METADATA_BLOCK_SIZE);
		return cregion;
	}

	/**
	 * Set the key for the metadata block.
	 * @param key Key for metadata block
	 */
	public void setKey(byte[] key)
	{
		this.mbuf.put(key,
			      0,
			      HrfsDisk.METADATA_KEY_SIZE);
	}

	/**
	 * Set the location pointer to the specified value.
	 * @param locptr Location on disk.
	 */
	public void setDataLocation(long locptr)
	{
		this.mbuf.putLong(HrfsDisk.METADATA_KEY_SIZE,
				  locptr);
	}

	/**
	 * Get the location pointer to the specified value.
	 * @return locptr Pointer to location within disk
	 */
	public long getDataLocation()
	{
		return this.mbuf.getLong(HrfsDisk.METADATA_KEY_SIZE);
	}
}
