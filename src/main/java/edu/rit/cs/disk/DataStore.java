/**
 * Copyright © 2015
 * Hadoop Replicating File System
 *
 * Sub-block store that is used to store data blocks on disk within the
 * filesystem. This is in contrast to the metadata store, which is
 * for bookeeping where each block is within this store.
 *
 * @file DataStore.java
 * @author Will Dignazio <wdignazio@gmail.com>
 */
package edu.rit.cs.disk;

import java.util.concurrent.Future;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.Files;
import java.nio.channels.FileChannel;
import java.nio.MappedByteBuffer;
import java.nio.ByteBuffer;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.io.IOException;

class DataStore
	extends BlockStore<DataBlock>
{
	public static final int DATA_BLOCK_SIZE		= 1024*64; // 64 kib

	/** Must use a backing file */
	private DataStore() { }

	/**
	 * Build a datastore object, open a channel and buffer to the backing
	 * file.
	 * @param path Path Path to backing file.
	 */
	public DataStore(Path path)
		throws FileNotFoundException, IOException
	{
		super(path);
	}

	/**
	 * Format the block storage, in this implementation, does nothing.
	 * 
	 * XXX In the future, more laborious tasks may need to be done for
	 * formatting a data disk. For now, all data block management is
	 * handled by the MetaStore, and formatting that is effectively
	 * destroying all data known to this store.
	 */
	@Override
	public void format()
		throws IOException { }

	/**
	 * Maps an integer to a block within the data store, the block location
	 * on disk is derived from the block size of the store.
	 * @param didx Data block index
	 * @return buffer ByteBuffer backed by data block
	 */
	private ByteBuffer getDataBlockMap(long didx)
		throws IOException
	{
		MappedByteBuffer buffer;
		long blkaddr;

		/* Translate to correct byte offset */
		blkaddr = didx * DATA_BLOCK_SIZE;
		buffer = this.getChannel().map(FileChannel.MapMode.READ_WRITE,
				    DATA_BLOCK_SIZE,
				    blkaddr);

		return buffer;
	}
	
	/**
	 * Insert a data block into the store.
	 * @param key Block key
	 * @param blk BLock of data
	 * @return success Whether insertion was successful.
	 */
	@Override
	public Future<DataBlock> insert(byte[] key, byte[] blk)
		throws IOException
	{
		ByteBuffer kbuf;
		ByteBuffer blkbuf;
		byte[] iblkdat;
		long blkn;

		if(key.length != HrfsDisk.LONGSZ)
			throw new IllegalArgumentException("Invalid Key Size");
		if(blk.length != DATA_BLOCK_SIZE)
			throw new IllegalArgumentException("Invalid Block Size");

		/* XXX Not ideal, but until we make an object based store */
		kbuf = ByteBuffer.wrap(key);
		blkn = kbuf.getLong();

		/* Retrieve a mapping to disk, flush data buffer */
		blkbuf = getDataBlockMap(blkn);
		blkbuf.put(blk, 0, DATA_BLOCK_SIZE);

		return null;
	}

	/**
	 * Get a block of data from the store.
	 * @param key Key for block
	 * @return data Block of data
	 */
	@Override
	public Future<DataBlock> get(byte[] key)
		throws IOException
	{
		return null;
	}

	/**
	 * Remove a block of data from the store.
	 * @param key Block key
	 * @return Whether removal was successful
	 */
	@Override
	public Future<Boolean> remove(byte[] key)
		throws IOException
	{
		return null;
	}
}
