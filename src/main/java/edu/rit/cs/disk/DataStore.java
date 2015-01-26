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

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
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
	public static final int DEFAULT_WORKER_COUNT	= 4;
	public static final int DEFAULT_WORKQ_SIZE	= 100;

	private ExecutorService wpool;
	private BlockingQueue<Runnable> wqueue;

	/**
	 * Helper class for submissions of worker jobs that needs to put data on
	 * disk. This performs the insertion procedure that actually copies the
	 * given byte buffer.
	 */
	private class InsertCallback
		implements Callable<DataBlock>
	{
		private long _blkn;
		private byte[] _blk;

		/**
		 * Constructor that takes the block number within the store and
		 * produces a callback object that will yield the result of a
		 * future promising a DataBlock.
		 * @param blkn Block number to insert data into
		 * @param blk Block data
		 */
		public InsertCallback(long blkn, byte[] blk)
		{
			super();
			this._blkn = blkn;
			this._blk = blk;
		}

		/**
		 * Runs the insertion routine for this store, and returns a block
		 * object as the promised value.
		 * @return dblk Block with backing disk page.
		 */
		@Override
		public DataBlock call()
			throws Exception
		{
			DataBlock dblk;
			ByteBuffer blkbuf;

			/* Retrieve a mapping to disk, flush data buffer */
			blkbuf = getDataBlockMap(this._blkn);
			blkbuf.put(this._blk, 0, DATA_BLOCK_SIZE);
			dblk = new DataBlock(blkbuf, _blkn);

			return dblk;
		}
	}

	/** Must use a backing file */
	private DataStore() { }

	/**
	 * Build a datastore object, open a channel and buffer to the backing
	 * file. This sets the worker pool and work queue to a default constant
	 * that is set at compile time.
	 * @param path Path Path to backing file.
	 */
	public DataStore(Path path)
		throws FileNotFoundException, IOException
	{
		super(path);

		/*
		 * Create a bounded work queue for submissions of store jobs.
		 * If the queue size is reached in the wqueue, then the calling
		 * Thread itself will perform the task.
		 */
		this.wqueue = new ArrayBlockingQueue<Runnable>(DEFAULT_WORKQ_SIZE);
		this.wpool = new ThreadPoolExecutor(DEFAULT_WORKER_COUNT, // Core Pool Size
						    DEFAULT_WORKER_COUNT, // Max Pool Size
						    Long.MAX_VALUE,
						    TimeUnit.SECONDS,
						    wqueue,
						    new ThreadPoolExecutor.CallerRunsPolicy());
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
					       blkaddr,
					       DATA_BLOCK_SIZE);
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
		Callable<DataBlock> dcall;
		ByteBuffer kbuf;
		long blkn;

		if(isClosed() == true)
			throw new IOException("Store is closed");
		
		if(key.length != HrfsDisk.LONGSZ)
			throw new IllegalArgumentException("Invalid Key Size");
		if(blk.length != DATA_BLOCK_SIZE)
			throw new IllegalArgumentException("Invalid Block Size");

		/* XXX Not ideal, but until we make an object based store */
		kbuf = ByteBuffer.wrap(key);
		blkn = kbuf.getLong();
		dcall = new InsertCallback(blkn, blk);

		return wpool.submit(dcall);
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

	/**
	 * Close this block store.
	 */
	@Override
	public void close()
		throws IOException
	{
		super.close();
		this.wpool.shutdown();
	}
			
}
