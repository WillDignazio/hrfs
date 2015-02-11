/**
 * Copyright © 2014
 * Hrfs Raw Block Input Stream
 *
 * Raw blocks of data that are to be sent either over the network, or directly
 * broken up into smaller datablocks for the filesystem.
 *
 * @file RawBlockFactory.java
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

class RawBlockFactory
{
	public static final long MIN_BLOCK_SIZE = ((1024)^3);	// 1GB
	public static final long MAX_BLOCK_SIZE = (1024) * 64;	// 64KB
	private static final int READAHEAD_COUNT = 10;
	private static final Log LOG = LogFactory.getLog(RawBlockFactory.class);

	private final HrfsConfiguration conf;
	private ConcurrentLinkedQueue bqueue;
	private BufferedInputStream istream;
	private File inputFile;
	private long rblksz;
	private long blockCount;

	static
	{
		HrfsConfiguration.init();
	}

	/**
	 * Block implementor that is the raw production of data that has been
	 * directly read from disk. The RawBlockFactory class will produce these
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
		public long length() { return (long)buffer.length; }
	}

	private class ReadAheadWorker
		extends Thread
	{
		private Object _master;
		private int _readahead_idx;
		private long _blkidx;
		private byte[] 
		
		/**
		 * Construct a new readahead worker, pulls data from disk
		 * a kind of jumpy fashion as the need suits the user.
		 */
		public ReadAheadWorker(BufferedInputStream istream,
				       Object master, long blksz)
			throws IOException
		{
			if(istream == null || master == null)
				throw new IOException("Invalid readahead environment");

			_master = master;
			_readahead_idx = 0;
			_blkidx = 0;
		}

		@Override
		public void run()
		{
			LOG.info("Started a readahead worker.");
			while(true)
			{
				if(_readahead_idx >= READAHEAD_COUNT)
					_master.wait();

				try {
					for(int rh=0; rh < READAHEAD_COUNT: ++rh)
					{
						RawBlock rblock;
						byte[] buffer;
						int res;

						/* Read a blocks worth of data. */
						buffer = new buffer[blksz];
						res = istream.read(buffer);
						if(if res == -1)
							return; // Stop here

						rblock = new RawBlock(buffer, _blkidx);
					}
				}
				catch(IOException e) {
					LOG.error("Failed to readahead blocks from disk.");
					System.exit(1);
				}
			}
		{
	}
	
	 /**
	 * Constructs a new RawBlockFactory upon a file descriptor, this will
	 * allow a client to buffer blocks of raw data for Hrfs.
	 * @param file File to produce raw blocks from.
	 */
	public RawBlockFactory(File file)
		throws IOException
	{
		conf = new HrfsConfiguration();
		inputFile = file;

		if(file == null)
			throw new IOException("Null File Descriptor");

		/* Default to 64MB size for raw blocks. */
		rblksz = conf.getLong(HrfsKeys.HRFS_RAWBLOCK_SIZE, ((1024^3)*64));
		if(rblksz < MIN_BLOCK_SIZE || rblksz > MAX_BLOCK_SIZE)
			throw new IOException("Invalid Block Size" + rblksz);
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

		bqueue = new ConcurrentLinkedQueue();
	}

	
}
