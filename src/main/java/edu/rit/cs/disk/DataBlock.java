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
	/**
	 * Build a data block object, careful not to break immutability.
	 * @param buffer Backing buffer of DataBlock
	 */
	public DataBlock(ByteBuffer buffer)
	{
		super(buffer);
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
}
