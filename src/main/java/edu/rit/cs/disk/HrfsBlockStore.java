/**
 * Copyright © 2015
 * Hadoop Replicating File System
 *
 * Interface that allows a running hrfs instance to store
 * blocks on disk, and likewise retrieve them.
 *
 * @file HrfsBlockStore.java
 * @author Will Dignazio <wdignazio@gmail.com>
 */
package edu.rit.cs.disk;

import java.io.IOException;

public interface HrfsBlockStore
{
	/**
	 * Format the block storage such that no data appears 
	 * to be in it. Implementors of this function shall not
	 * allow blocks previous to format to be retrieved.
	 */
	public void format()
		throws IOException;

	/**
	 * Insert a block of data into the storage unit.
	 * @param key Block key
	 * @param blk Block of data
	 * @return success Whether insertion was successful
	 */
	public boolean insert(byte[] key, byte[] data)
		throws IOException;

	/**
	 * Get a block data from the storage unit.
	 * @param key Key for block
	 * @return data Block of data
	 */
	public byte[] get(byte[] key)
		throws IOException;

	/**
	 * Remove a block of data from the storage unit.
	 * @param key Block key
	 * @return Whether removal was successful
	 */
	public boolean remove(byte[] key)
		throws IOException;
}
