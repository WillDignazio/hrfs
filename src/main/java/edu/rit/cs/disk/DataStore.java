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
	implements HrfsBlockStore
{
	public static final int DATA_BLOCK_SIZE		= 1024*64; // 64 kib

	private Path dPath;
	private RandomAccessFile dFile;
	private FileChannel dChannel;

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
		this.dPath = path;
		this.dFile = new RandomAccessFile(dPath.toFile(), "rw");
		this.dChannel = dFile.getChannel();
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
	private ByteBuffer getDataBlockMap(int didx)
		throws IOException
	{
		MappedByteBuffer buffer;
		long blkaddr;

		/* Translate to correct byte offset */
		blkaddr = didx * DATA_BLOCK_SIZE;
		buffer = dChannel.map(FileChannel.MapMode.READ_WRITE,
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
	public boolean insert(byte[] key, byte[] data)
		throws IOException
	{
		long blkaddr;

		if(key.length != HrfsDisk.LONGSZ)
			throw new IOException("Invalid Key Size");
		if(data.length != DATA_BLOCK_SIZE)
			throw new IOException("Invalid Block Size");

		return false;
	}

	/**
	 * Get a block of data from the store.
	 * @param key Key for block
	 * @return data Block of data
	 */
	@Override
	public byte[] get(byte[] key)
		throws IOException
	{
		return new byte[1];
	}

	/**
	 * Remove a block of data from the store.
	 * @param key Block key
	 * @return Whether removal was successful
	 */
	@Override
	public boolean remove(byte[] key)
		throws IOException
	{
		return false;
	}
}
