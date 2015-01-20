/**
 * Copyright © 2015
 * Hadoop Replicating File System
 *
 * This file contains an API for writing and reading blocks of data
 * from an on disk structure. For now, this is solidly a B+ Tree implementation,
 * that will serve as the backend for HRFS clients.
 *
 * On Disk Data Format:
 *
 * Disk:
 *
 * +---------------+ +-------------------------------------+
 * | Metadata File | |         Data File     	           |
 * +---------------+ +-------------------------------------+
 *      |                     |
 *      |           +-----------------------------+
 *      |           | DataBlock | DataBlock | ... |
 *      |           +-----------------------------+
 * +-----------------------------------------------------+
 * | MetadataExtent |  MetadataExtent  |  ...  |    |    |
 * +-----------------------------------------------------+
 * 0            4096         |
 *                           |
 *                     +-------------------------------+
 *                     | MetadataBlock | ... | ...     |
 *                     +-------------------------------+
 *                     0     |        64
 *                           |
 *               +---------------------------------+
 *               |  Key  |   BlkAddr  |  [Ptrs]    |
 *               +---------------------------------+
 *               0      20           28           64
 *
 * @file HrfsDisk.java
 * @author Will Dignazio <wdignazio@gmail.com>
 */
package edu.rit.cs.disk;

import java.util.Random;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.channels.FileChannel;
import java.nio.MappedByteBuffer;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.io.IOException;

public class HrfsDisk
	implements HrfsBlockStore
{
	public static final int LONGSZ = (Long.SIZE / Byte.SIZE);
	public static final int DATA_BLOCK_SIZE		= 1024*64; // 64 kib

	private Path mblkPath;
	private Path dblkPath;
	private FileChannel dChannel;

	private SuperBlock sb;
	private MetaStore metastore;

	/** No default constructor */
	private HrfsDisk() { }
	
	/**
	 * Build a Disk object for the filesystem, this will be the interface
	 * object for all on disk data structures and data objects.
	 * @param path Path to on disk file for storage
	 */
	public HrfsDisk(Path mblkPath, Path dblkPath)
		throws FileNotFoundException, IOException
	{
		long rootAddr;

		if(Files.notExists(dblkPath, LinkOption.NOFOLLOW_LINKS))
			throw new FileNotFoundException(dblkPath.toString());

		this.mblkPath = mblkPath;
		this.dblkPath = dblkPath;
		this.dChannel = new RandomAccessFile(dblkPath.toFile(), "rw").getChannel();

		this.metastore = new MetaStore(mblkPath);
	}

	/**
	 * Formats the disk to have no known data blocks, this effectively
	 * erases the content on disk.
	 *
	 * NOTE: This does _not_ zero _all_ data on the disk.
	 */
	@Override
	public void format()
		throws IOException
	{
		metastore.format();
	}

	/**
	 * Inserts a block of data into the on disk storage.
	 * @param key Key of block
	 * @param blk Block of data
	 * @return success Whether insertion was successful
	 */
	@Override
	public boolean insert(byte[] key, byte[] data)
		throws IOException
	{
		boolean mins;

		System.out.println("Inserting: " + key.toString());

		/*
		 * XXX Temporary
		 * For now, we're going to create an in memory allocation
		 * of block data. This is until we have a working data
		 * block allocator.
		 */
		mins = metastore.insert(key, new byte[DATA_BLOCK_SIZE]);

		return mins;
	}

	/**
	 * Gets a block of data from the on disk storage.
	 * @param key Key for block of data.
	 * @return data Block of data.
	 */
	public byte[] get(byte[] key)
		throws IOException
	{
		return new byte[1];
	}

	/**
	 * Removes a block of data from the on disk storage.
	 * @param key Key of block
	 * @return Whether removal was successful
	 */
	@Override
	public boolean remove(byte[] key)
		throws IOException
	{
		return false;
	}
	
	public static void main(String[] args)
		throws Exception
	{
		Random rand;
		byte[] dbuf;
		byte[] k1;
		HrfsDisk disk;

		rand = new Random();

		dbuf = new byte[HrfsDisk.DATA_BLOCK_SIZE];
		k1 = new byte[20];

		rand.nextBytes(dbuf);
		rand.nextBytes(k1);

		disk = new HrfsDisk(Paths.get("test-meta"), Paths.get("test-data"));
		disk.format();

		disk.insert(k1, dbuf);
	}
}
