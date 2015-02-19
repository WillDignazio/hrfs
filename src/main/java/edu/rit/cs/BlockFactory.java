/**
 * Copyright © 2015
 * Hrfs Block Factory
 *
 * Decomposes data sources into specified block sizes, each block is enumerated
 * according to the Block interface. Each block produced will have a constant
 * size specified at the construction of the factory.
 *
 * @file BlockFactory.java
 * @author Will Dignazio <wdignazio@gmail.com>
 */
package edu.rit.cs;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

import edu.rit.cs.HrfsConfiguration;

public class BlockFactory
{
	public static final long MAX_BLOCK_SIZE = 1024L*1024L*1024L;// 1GB
	public static final long MIN_BLOCK_SIZE = 1024L * 64L;	// 64KB
	private static final int READAHEAD_COUNT = 10;
	private static final Log LOG = LogFactory.getLog(BlockFactory.class);

	private final HrfsConfiguration conf;
	private ConcurrentLinkedQueue<FactoryBlock> bqueue;
	private ReadAheadWorker rworker;
	private long blockCount;
	private AtomicBoolean done;	
	private AtomicLong produced;
	private int blksz;

	static
	{
		HrfsConfiguration.init();
	}

	/**
	 * Block implementor that is the raw production of data that has been
	 * directly read from disk. The BlockFactory class will produce these
	 * objects as Block interface adherents, and will allow anyone wishing
	 * to use them for network or otherwise easily.
	 */
	private class FactoryBlock
		implements Block
	{
		private long _idx;
		private byte[] _buffer;

		/**
		 * Produce a new raw block, which must have at least a backing
		 * buffer and an index number. The index of the block is determined
		 * by the configured block size within hadoop. The block size will
		 * determine how fast the index iterates, and when the readahead
		 * thread grabs more data from disk.
		 *
		 * @param buffer Backing buffer of the FactoryBlock, usually in memory
		 * @param idx Index of the block, where it lies on disk.
		 */
		public FactoryBlock(byte[] buffer, long idx)
		{
			_idx = idx;
			_buffer = buffer;
		}
		
		@Override
		public long length()
		{ return (long)_buffer.length; }

		@Override
		public long index()
		{ return _idx; };

		@Override
		public byte[] data()
		{ return _buffer; }
	}

	/**
	 * XXX Needs more intelligent tracking
	 *
	 * When the readahead gets a signal to do work, it buffers up the next
	 * READAHEAD_COUNT amount of blocks. This allows a client to get a
	 * substantial performance boost, but is dependant on the client reading
	 * all the buffered blocks before reading more.
	 */
	private class ReadAheadWorker
		extends Thread
	{
		private int _readahead_idx;
		private long _blkidx;
		private int _blksz;
		private Object _master;
		private InputStream _istream;
		private Runtime _runtime;
		
		/**
		 * Construct a new readahead worker, pulls data from disk
		 * a kind of jumpy fashion as the need suits the user.
		 */
		public ReadAheadWorker(InputStream istream, Object master, int blksz)
			throws IOException
		{
			if(istream == null)
				throw new IOException("Invalid readahead environment");

			_readahead_idx = 0;
			_blkidx = 0;
			_blksz = blksz;
			_master = master;
			_istream = istream;
			_runtime = Runtime.getRuntime();
			System.out.println("Started Readahead worker");
			System.out.println(" -- Aware of " + _runtime.totalMemory() + " limit");
			System.out.println(" -- Currently sitting at " + _runtime.freeMemory());
		}

		@Override
		public void run()
		{
			LOG.info("Started a readahead worker.");
			while(true)
			{
				try {
					/*
					 * If the readahead value is above the threshold,
					 * then stop here and wait for the client to
					 * flush what we've got. Then build some more.
					 * We're also going to be aware of our memory limit, not
					 * being too greedy with it.
					 */
					if(_readahead_idx >= READAHEAD_COUNT ||
					   _runtime.freeMemory() < (_blksz * 2)) {
						synchronized(_master) {
							_master.wait();
						}
						_readahead_idx = 0;
						System.out.println("Reset readahead count");
					}
					
					for(int rh=0; rh < READAHEAD_COUNT; ++rh)
					{
						FactoryBlock rblock;
						byte[] buffer;
						int res;

						/* Read a blocks worth of data. */
						buffer = new byte[_blksz];
						res = _istream.read(buffer);
						if(res == -1) {
							System.out.println("Readahead finished");
							done.set(true);
							return; // Stop here
						}

						rblock = new FactoryBlock(buffer, _blkidx);
						bqueue.add(rblock);

						++_blkidx;
						++_readahead_idx;
						System.out.println("Readahead idx: " + _readahead_idx);
					}
				}
				catch(OutOfMemoryError e) {
					System.out.println("Hit memory cap, backing down...");
					try {
						Thread.sleep(1000);
						synchronized(_master) { _master.wait(); }
					}
					catch(InterruptedException e2) { continue; }
				}
				catch(IOException e) {
					LOG.error("Failed to readahead blocks from disk.");
					System.exit(1);
				}
				catch(InterruptedException e) {
					LOG.error("ReadAhead worker was interrupted before " +
						  "completing it's task.");
					System.exit(1);
				}
				
			}
		}
	}

	/**
	 * Default constructor that instantiates the necessary data structures.
	 */
	private BlockFactory(int blksz)
		throws IOException
	{
		conf = new HrfsConfiguration();
		done = new AtomicBoolean(false);
		produced = new AtomicLong(0);
		blksz = blksz;

		if(blksz < MIN_BLOCK_SIZE || blksz > MAX_BLOCK_SIZE)
			throw new IOException("Invalid Block Size: " + blksz);
		if(blksz % 4096 != 0 && blksz % 512 != 0)
			throw new IOException("Block size is misaligned, " +
					      "use multiples of 4096 or 512 bytes");

		bqueue = new ConcurrentLinkedQueue<FactoryBlock>();
	}

	/**
	 * Constructs a new BlockFactory from a byte array. This will produce
	 * blocks that are complete copies of data from the byte array, and
	 * changes to the underlying byte array will not affect blocks produced.
	 */
	public BlockFactory(byte[] barr, int blksz)
		throws IOException
	{
		this(blksz);

		InputStream istream;

		if(barr == null)
			throw new IOException("Null byte array input");

		blockCount = barr.length / blksz;
		if((barr.length % blksz != 0) || blockCount == 0)
		   ++blockCount;

		LOG.info("Breaking byte input array into " + blockCount
			 + " blocks");
		istream = new ByteArrayInputStream(barr);
		
		rworker = new ReadAheadWorker(istream, this, blksz);
		rworker.start();
	}

	/**
	 * Constructs a new BlockFactory upon a file descriptor, this will
	 * allow a client to buffer blocks of raw data for Hrfs.
	 * @param file File to produce raw blocks from.
	 * @param blksz Size of blocks produced
	 */
	public BlockFactory(File file, int blksz)
		throws IOException
	{
		this(blksz);

		InputStream istream;

		if(file == null)
			throw new IOException("Null File Descriptor");

		if(!file.exists())
			throw new FileNotFoundException();

		LOG.info("Opening new raw block producer from file "
			 + file.getName());

		blockCount = (file.length() / blksz);
		if((file.length() % blksz) != 0 || blockCount == 0)
			++blockCount; // At least a block count of 1.

		LOG.info("Breaking raw file into " + blockCount + " blocks");
		istream = new BufferedInputStream(
			new FileInputStream(file));

		rworker = new ReadAheadWorker(istream, this, blksz);
		rworker.start();
	}

	/**
	 * Return the expected block count for the factory.
	 * @return Number of blocks this factory will produce.
	 */
	public long blockCount()
	{ return blockCount; };

	/** 
	 * Return the number of blocks that have been produced.This method is
	 * _not_ thread safe, this derives from how users of the factory may
	 * asynchronously request blocks to be produced.
	 * @return Number of blocks produced from the factory.
	 */
	public long blocksProduced()
	{ return produced.get(); }

	/**
	 * Return whether we've reached the data input.
	 * @return Whether the factory will produce any more blocks.
	 */
	public boolean isDone()
	{ return done.get(); }
	
	/**
	 * Produces a block of data for a client application, this block is
	 * sequential, gauranteed to be the previous block compared to the 
	 * next block in the queue.
	 * @return A block of data, whose size matches the factories construction value.
	 */
	public Block getBlock()
	{
		Block blk;

		for(;;) {
			/* Wake up the readahead worker */
			if(bqueue.isEmpty() == true)
				synchronized(this) { this.notifyAll(); }

			blk = bqueue.poll();
			if(blk == null) {
				if(done.get()) {
					System.out.println("Finished Block Factory");
					return null;
				}

				/* if !done, we're waiting on readahead */
				continue;
			}

			produced.getAndIncrement();
			return blk;
		}
	}
}
