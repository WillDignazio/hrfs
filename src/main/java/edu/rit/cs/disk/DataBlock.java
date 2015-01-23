/**
 * Copyright © 2015
 * Hadoop Replicating File System
 *
 * Basic placeholder object for data block on disk. Allows a holder to view the
 * contents of the block, and copy its contents. The intention of the datablock
 * is to be an immutable data structure on disk, and can only be viewed,
 * inserted, or removed from disk storage.
 *
 * @file DataBlock.java
 * @author William Dignazio <wdignazio@gmail.com>
 */
package edu.rit.cs.disk;

import java.nio.ByteBuffer;

final class DataBlock
	extends Block
{
	private final long _idx;

	/**
	 * Build a data block object, careful not to break immutability.
	 * @param buffer Backing buffer of DataBlock
	 * @param blkn Block index number
	 */
	public DataBlock(ByteBuffer buffer, long blkn)
	{
		super(buffer);
		this._idx = blkn;
	}

	/**
	 * Get the size of the datablock in bytes.
	 * @return nbytes Size of datablock in bytes.
	 */
	@Override
	public long size()
	{
		return DataStore.DATA_BLOCK_SIZE;
	}

	/**
	 * Get the block index of this data block.
	 * @return blkn Data block index
	 */
	public long getBlockIndex()
	{
		return this._idx;
	}
}
