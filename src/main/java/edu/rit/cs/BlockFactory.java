/**
 * Copyright © 2014
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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

import edu.rit.cs.HrfsConfiguration;

class BlockFactory
{
	public static final long MAX_BLOCK_SIZE = 1024L*1024L*1024L;// 1GB
	public static final long MIN_BLOCK_SIZE = 1024L * 64L;	// 64KB
	private static final int READAHEAD_COUNT = 10;
	private static final Log LOG = LogFactory.getLog(BlockFactory.class);

	private final HrfsConfiguration conf;
	private ConcurrentLinkedQueue<RawBlock> bqueue;
	private ReadAheadWorker rworker;
	private BufferedInputStream istream;
	private boolean eof;
	private File inputFile;
	private long blockCount;
	private int rblksz;

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
	private class RawBlock
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
		 * @param buffer Backing buffer of the RawBlock, usually in memory
		 * @param idx Index of the block, where it lies on disk.
		 */
		public RawBlock(byte[] buffer, long idx)
		{
			_idx = idx;
			_buffer = buffer;
		}
		
		@Override
		public long length() { return (long)_buffer.length; }

		@Override
		public long index() { return _idx; };
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
		
		/**
		 * Construct a new readahead worker, pulls data from disk
		 * a kind of jumpy fashion as the need suits the user.
		 */
		public ReadAheadWorker(BufferedInputStream istream, Object master, int blksz)
			throws IOException
		{
			if(istream == null)
				throw new IOException("Invalid readahead environment");

			_readahead_idx = 0;
			_blkidx = 0;
			_blksz = blksz;
			_master = master;
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
					 */
					if(_readahead_idx >= READAHEAD_COUNT) {
						synchronized(_master) {
							_master.wait();
						}
						_readahead_idx = 0;
					}
					
					for(int rh=0; rh < READAHEAD_COUNT; ++rh)
					{
						RawBlock rblock;
						byte[] buffer;
						int res;

						/* Read a blocks worth of data. */
						buffer = new byte[_blksz];
						res = istream.read(buffer);
						if(res == -1) {
							eof = true;
							return; // Stop here
						}

						rblock = new RawBlock(buffer, _blkidx);
						bqueue.add(rblock);

						++_blkidx;
						++_readahead_idx;						
					}
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
	 * Constructs a new BlockFactory upon a file descriptor, this will
	 * allow a client to buffer blocks of raw data for Hrfs.
	 * @param file File to produce raw blocks from.
	 */
	public BlockFactory(File file, int blksz)
		throws IOException
	{
		conf = new HrfsConfiguration();
		inputFile = file;
		eof = false;

		if(file == null)
			throw new IOException("Null File Descriptor");

		/* Default to 64MB size for raw blocks. */
		rblksz = blksz;

		if(rblksz < MIN_BLOCK_SIZE || rblksz > MAX_BLOCK_SIZE)
			throw new IOException("Invalid Block Size: " + rblksz);
		if(rblksz % 4096 != 0 && rblksz % 512 != 0)
			throw new IOException("Block size is misaligned, " +
					      "use multiples of 4096 or 512 bytes");

		if(!file.exists())
			throw new FileNotFoundException();

		LOG.info("Opening new raw block producer from file "
			 + file.getName());

		blockCount = (file.length() / rblksz);
		if(blockCount == 0)
			++blockCount; // At least a block count of 1.

		LOG.info("Breaking raw file into " + blockCount + " blocks");
		istream = new BufferedInputStream(
			new FileInputStream(file));

		bqueue = new ConcurrentLinkedQueue<RawBlock>();
		rworker = new ReadAheadWorker(istream, this, rblksz);

		rworker.start();
	}

	/** Return whether we've reached the eof */
	public boolean isEOF()
	{ return eof; }
	
	/**
	 * Produces a block of data for a client application, this block is
	 * sequential, gauranteed to be the previous block compared to the 
	 * next block in the queue.
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
				if(eof)
					return null;
				/* if !eof, we're waiting on readahead */
				continue;
			}

			return blk;
		}
	}
}

