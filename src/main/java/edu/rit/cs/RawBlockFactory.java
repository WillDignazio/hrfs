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
	private static final Log LOG = LogFactory.getLog(RawBlockFactory.class);


	private final HrfsConfiguration conf;
	private ConcurrentLinkedQueue bqueue;
	private BufferedInputStream istream;
	private File inputFile;
	private long rblksz;
	private long blockIdx;
	private long blockCount;

	static
	{
		HrfsConfiguration.init();
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
