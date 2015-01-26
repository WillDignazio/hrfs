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
import java.util.ArrayList;
import java.util.concurrent.Future;
import java.util.concurrent.ExecutionException;
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
	extends BlockStore<DataBlock>
{
	public static final int LONGSZ = (Long.SIZE / Byte.SIZE);

	private SuperBlock sb;
	private MetaStore metastore;
	private DataStore datastore;

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
		super();
		this.metastore = new MetaStore(mblkPath);
		this.datastore = new DataStore(dblkPath);
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
		datastore.format();
	}

	/**
	 * Inserts a block of data into the on disk storage.
	 * @param key Key of block
	 * @param blk Block of data
	 * @return success Whether insertion was successful
	 */
	@Override
	public Future<DataBlock> insert(byte[] key, byte[] data)
		throws IOException
	{
		System.out.println("Inserting: " + key.toString());

		byte[] tb = new byte[LONGSZ];
		return datastore.insert(tb, data);
	}

	/**
	 * Gets a block of data from the on disk storage.
	 * @param key Key for block of data.
	 * @return data Block of data.
	 */
	@Override
	public Future<DataBlock> get(byte[] key)
		throws IOException
	{
		return null;
	}

	/**
	 * Removes a block of data from the on disk storage.
	 * @param key Key of block
	 * @return Whether removal was successful
	 */
	@Override
	public Future<Boolean> remove(byte[] key)
		throws IOException
	{
		return null;
	}

	/**
	 * Close this Hrfs primary block store.
	 */
	@Override
	public synchronized void close()
		throws IOException
	{
		if(isClosed())
			throw new IOException("Store already closed");

		super.close();
		this.datastore.close();
		this.metastore.close();
	}
	
	public static void main(String[] args)
		throws InterruptedException, ExecutionException
	{
		Random rand;
		byte[] dbuf;
		byte[] dkey;
		Future<DataBlock> future;
		HrfsDisk disk;

		rand = new Random();
		dbuf = new byte[DataStore.DATA_BLOCK_SIZE];
		dkey = new byte[MetaStore.METADATA_KEY_SIZE];

		try {
			disk = new HrfsDisk(Paths.get("test-meta"), Paths.get("test-data"));
			disk.format();

			rand.nextBytes(dbuf);
			rand.nextBytes(dkey);

			future = disk.insert(dkey, dbuf);
			future.get();

			disk.close();
		}
		catch(FileNotFoundException e) {
			System.err.println("Test file not present: " + e.toString());
		}
		catch(IOException e) {
			System.err.println("Error creating stores: " + e.toString());
		}
	}
}
