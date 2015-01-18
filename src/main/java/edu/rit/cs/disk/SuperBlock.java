/**
 * Copyright © 2015
 * Hadoop Replicating File System
 *
 * The in memory representation of the on disk superblock, this block contains
 * the top level node for the on disk tree structure.
 *
 * On disk format:
 * 0                                                                          64K 
 * +----------------------------------------------------------------------------+
 * | Boot Sector | RooBlk | WrIdx | ExCnt | ExAv |                              |
 * +----------------------------------------------------------------------------+
 *
 * @file SuperBlock.java
 * @author Will Dignazio <wdignazio@gmail.com>
 */
package edu.rit.cs.disk;

import java.nio.ByteBuffer;

class SuperBlock
{
	private static final int LONGSZ = (Long.SIZE / Byte.SIZE);
	private static final int INTSZ = (Integer.SIZE / Byte.SIZE);
	
	private static final int BOOTSECTOR_SIZE = 512;
	private static final int ROOTBLOCK_OFFSET = BOOTSECTOR_SIZE;
	private static final int WRITEINDEX_OFFSET = BOOTSECTOR_SIZE + LONGSZ;
	private static final int EXTENT_COUNT_OFFSET = WRITEINDEX_OFFSET + LONGSZ;
	private static final int EXTENT_AVAIL_OFFSET = EXTENT_COUNT_OFFSET + LONGSZ;

	public static final int SUPER_MAGIC = 0xCAFEBABE;

	private ByteBuffer mbuf;

	public SuperBlock(ByteBuffer mbuf)
	{
		this.mbuf = mbuf;
	}

	/**
	 * Check the magic number for the superblock, if this fails,
	 * the superblock has become invalid.
	 * @return valid True for valid, False for invalid
	 */
	public boolean isValid()
	{
		long magic;

		magic = this.mbuf.getLong(HrfsDisk.SUPERBLOCK_SIZE -
					  LONGSZ);

		if(magic == SUPER_MAGIC)
			return true;

		return false;
	}

	/**
	 * Gets the root metadata block address from the superblock, this is
	 * the first key of the node in the on disk b+ tree structure.
	 */
	public long getRootBlockAddress()
	{
		long addr;

		addr = this.mbuf.getLong(ROOTBLOCK_OFFSET);
		return addr;
	}

	/**
	 * Sets the root metadata block address of the superblock.
	 * @param addr Address of the root metadata block.
	 */
	public void setRootBlockAddress(long addr)
	{
		this.mbuf.putLong(ROOTBLOCK_OFFSET, addr);
	}
	
	/**
	 * Set the superblock magic value, this will make it such
	 * that this superblock will pass a validity test.
	 * @param m Magic value
	 */
	public void setMagic(long m)
	{
		this.mbuf.putLong(HrfsDisk.SUPERBLOCK_SIZE -
				  LONGSZ, m);
	}

	/**
	 * Set the metadata extent count for the store.
	 * @param count Number of extents in store.
	 */
	public void setMetadataExtentCount(int count)
	{
		this.mbuf.putInt(EXTENT_COUNT_OFFSET, count);
	}

	/**
	 * Get the metadata extent count for the store.
	 * @return Number of metadata extents
	 */
	public int getMetadataExtentCount()
	{
		return this.mbuf.getInt(EXTENT_COUNT_OFFSET);
	}

	/**
	 * Set the number of available metadata extents.
	 * @param count Number of available metadata extents.
	 */
	public void setMetadataExtentAvailable(int count)
	{
		this.mbuf.putInt(EXTENT_AVAIL_OFFSET, count);
	}

	/**
	 * Get the number of availalbe metadata extents.
	 * @return Number of available extents.
	 */
	public int getMetadataExtentsAvailable()
	{
		return this.mbuf.getInt(EXTENT_AVAIL_OFFSET);
	}
}
