/**
 * Copyright © 2015
 * Hadoop Replicating File System
 *
 * The in memory representation of the on disk superblock, this block contains
 * the top level node for the on disk tree structure.
 *
 * On disk format:
 * 0         8      16       24       32         40       48                   4K
 * +----------------------------------------------------------------------------+
 * | DblkIdx | MdIdx | BlkCnt | BlkAvail | Mroot  |  Magic | Unallocated        |
 * +----------------------------------------------------------------------------+
 *
 * @file SuperBlock.java
 * @author Will Dignazio <wdignazio@gmail.com>
 */
package edu.rit.cs.disk;

import java.nio.ByteBuffer;

class SuperBlock
{	
	private static final int DATA_INDEX_OFFSET = 0;
	private static final int META_INDEX_OFFSET = DATA_INDEX_OFFSET + HrfsDisk.LONGSZ;
	private static final int META_COUNT_OFFSET = META_INDEX_OFFSET + HrfsDisk.LONGSZ;
	private static final int META_AVAIL_OFFSET = META_COUNT_OFFSET + HrfsDisk.LONGSZ;
	private static final int META_ROOT_OFFSET = META_AVAIL_OFFSET + HrfsDisk.LONGSZ;

	public static final int SUPERBLOCK_OFFSET = META_ROOT_OFFSET + HrfsDisk.LONGSZ;
	public static final int SUPERBLOCK_SIZE = 4096;
	public static final int SUPER_MAGIC = 0xCAFEBABE;

	private ByteBuffer mbuf;

	public SuperBlock(ByteBuffer mbuf)
	{
		this.mbuf = mbuf;
	}

	/**
	 * Check the magic number for the superblock, if this fails, the
	 * superblock has become invalid.
	 * @return valid True for valid, False for invalid
	 */
	public boolean isValid()
	{
		long magic;

		magic = this.mbuf.getLong(SUPERBLOCK_SIZE -
					  HrfsDisk.LONGSZ);

		if(magic == SUPER_MAGIC)
			return true;

		return false;
	}

	/**
	 * Set the superblock magic value, this will make it such
	 * that this superblock will pass a validity test.
	 * @param m Magic value
	 */
	public void setMagic(long m)
	{
		this.mbuf.putLong(SUPERBLOCK_SIZE -
				  HrfsDisk.LONGSZ, m);
	}

	/**
	 * Set the metadata block count for the store.
	 * @param count Number of extents in store.
	 */
	public void setMetadataBlockCount(long count)
	{
		this.mbuf.putLong(META_COUNT_OFFSET, count);
	}

	/**
	 * Get the metadata block count for the store.
	 * @return Number of metadata extents
	 */
	public long getMetadataBlockCount()
	{
		return this.mbuf.getLong(META_COUNT_OFFSET);
	}

	/**
	 * Set the number of available metadata extents.
	 * @param count Number of available metadata extents.
	 */
	public void setMetadataBlockAvailable(long count)
	{
		this.mbuf.putLong(META_AVAIL_OFFSET, count);
	}

	/**
	 * Get the number of availalbe metadata extents.
	 * @return Number of available extents.
	 */
	public long getMetadataBlockAvailable()
	{
		return this.mbuf.getLong(META_AVAIL_OFFSET);
	}

	/**
	 * Get the MetadataBlock allocation index.
	 * @return long Index for next extent to allocate from.
	 */
	public long getMetadataBlockIndex()
	{
		return this.mbuf.getLong(META_INDEX_OFFSET);
	}

	/**
	 * Set the MetadataBlock allocation index.
	 * @param idx Index for next extent to allocate from.
	 */
	public void setMetadataBlockIndex(long idx)
	{
		this.mbuf.putLong(META_INDEX_OFFSET, idx);
	}

	/**
	 * Set the Metadata Root Block index.
	 * @param idx Root block index
	 */
	public void setMetadataRootIndex(long idx)
	{
		this.mbuf.putLong(META_ROOT_OFFSET, idx);
	}

	/**
	 * Get the Metadata Root Block index.
	 * @return idx Root block index
	 */
	public long getMetadataRootIndex()
	{
		return this.mbuf.getLong(META_ROOT_OFFSET);
	}

	/**
	 * Get the current data block allocation index.
	 * @return idx Data block allocation index.
	 */
	public long getDataBlockIndex()
	{
		return this.mbuf.getLong(DATA_INDEX_OFFSET);
	}

	/**
	 * Set the current data block allocation index.
	 * @param idx Index for next block to allocate.
	 */
	public void setDataBlockIndex(long idx)
	{
		this.mbuf.putLong(DATA_INDEX_OFFSET, idx);
	}
}
