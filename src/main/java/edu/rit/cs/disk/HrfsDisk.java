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
 *  Superblock
 *   |
 * +--+--------------------------------------------------+
 * |  | Metadata |              Data		         |
 * +--+--------------------------------------------------+
 * 0    |                     |
 *      |           +-----------------------------+
 *      |           | DataBlock | DataBlock | ... |
 *      |           +-----------------------------+
 * +-----------------------------------------------------+
 * | MetadataExtent |  MetadataExtent  |  ...  |    |    |
 * +-----------------------------------------------------+
 * 0            4096         |                       65536
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
	public static final int SUPERBLOCK_SIZE		= 4096;
	public static final int METADATA_EXTENT_SIZE	= 4096;
	public static final int METADATA_KEY_SIZE	= 20;
	public static final int METADATA_BLOCK_SIZE	= 64;
	public static final int DATA_BLOCK_SIZE		= 1024*64; // 64 kib

	private Path diskPath;
	private RandomAccessFile file;
	private FileChannel fchannel;
	private int mextCount;
	private long dataOffset;

	private SuperBlock sb;
	
	/**
	 * Build a Disk object for the filesystem, this will be the interface
	 * object for all on disk data structures and data objects.
	 * @param path Path to on disk file for storage
	 */
	public HrfsDisk(Path path)
		throws FileNotFoundException, IOException
	{

		if(Files.notExists(path, LinkOption.NOFOLLOW_LINKS))
			throw new FileNotFoundException(path.toString());

		this.file = new RandomAccessFile(path.toFile(), "rw");
		this.fchannel = file.getChannel();

		/* XXX must come after building channel + file obj */
		this.sb = getSuperBlock();
		
		/*
		 * This gives us the amount of data blocks that can be present in the
		 * disk. This will will be less as the reserved metadata section will
		 * consume some, but gives us a rough idea of how much space we need.
		 */
		double appx = (((double)file.length() - SUPERBLOCK_SIZE)
			       / (double)DATA_BLOCK_SIZE);
		
		System.out.println("Number of data blocks: " + appx);
		
		/* Amount of metadata bytes we need */
		double mbytes = appx * (double)METADATA_BLOCK_SIZE;
		System.out.println("Number of metadata bytes: " + mbytes);

		/* Then we find how many extents are going to cover the blocks */
		this.mextCount = (int)Math.ceil(mbytes / (double)METADATA_EXTENT_SIZE);
		if(this.mextCount == 0)
			++mextCount;
	}

	/**
	 * Gets the superblock of the on disk structure.
	 * @return sblock Superblock of the on disk structure.
	 */
	private SuperBlock getSuperBlock()
		throws IOException
	{
		MappedByteBuffer mbuf;
		SuperBlock sblock;

		mbuf = fchannel.map(FileChannel.MapMode.READ_WRITE,
				    0,
				    SUPERBLOCK_SIZE).load();

		sblock = new SuperBlock(mbuf);
		return sblock;
	}
	
	/**
	 * Retrieve a mapping to the numeric extent given to the method.
	 * @param exn Extent number
	 * @return mbuf MappedByteBuffer of extent
	 */
	private MetadataExtent getMetadataExtent(int exn)
		throws IOException
	{
		MetadataExtent ext;
		MappedByteBuffer mbuf;
		long exaddr;

		exaddr = METADATA_EXTENT_SIZE * exn;
		mbuf = fchannel.map(FileChannel.MapMode.READ_WRITE,
				    exaddr + SUPERBLOCK_SIZE,
				    exaddr + METADATA_EXTENT_SIZE).load();

		ext = new MetadataExtent(mbuf.duplicate(), exn);
		return ext;
	}

	/**
	 * Calculate the number of metadata blocks that are in the
	 * metadata section of the disk.
	 * @return exn Number of metadata extents
	 */
	public int getMetadataExtentCount()
	{
		return mextCount;
	}

	/**
	 * Formats the disk to have no known data blocks, this effectively
	 * erases the content on disk.
	 *
	 * NOTE: This does _not_ zero all data on the disk.
	 */
	@Override
	public void format()
		throws IOException
	{
		SuperBlock sblock;

		sblock = getSuperBlock();
		sblock.erase();

		for(int mext=0; mext < getMetadataExtentCount(); ++mext)
			getMetadataExtent(mext).erase();

		sblock.setMagic(SuperBlock.SUPER_MAGIC);
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
		System.out.println("Inserting: " + key.toString());
		MetadataExtent mext;
		MetadataBlock iblock;
		MetadataBlock qblock;
		long rootaddr;

		rootaddr = sb.getRootBlockAddress();
		if(rootaddr == 0) {
			System.out.println("Empty SuperBlock Root");
			mext = getMetadataExtent(0);
			qblock = mext.allocateMetadataBlock();

			return true;
		}
		else {
			System.out.println("Non-Empty SuperBlock Root");
			/* XXX Translate to extent N */
		}

		return false;
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
		
		disk = new HrfsDisk(Paths.get("test"));
		System.out.println("Extent Count: " + disk.getMetadataExtentCount());
		disk.format();

		disk.insert(k1, dbuf);
	}
}
