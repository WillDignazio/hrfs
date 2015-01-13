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
 * +--------------------------------------------------+
 * | Metadata |              Data		      |
 * +--------------------------------------------------+
 *      |                     |
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
{
	public static final int METADATA_EXTENT_SIZE	= 4096;
	public static final int METADATA_KEY_SIZE	= 20;
	public static final int METADATA_BLOCK_SIZE	= 64;
	public static final int DATA_BLOCK_SIZE		= 1024*64; // 64 kib

	private Path diskPath;
	private RandomAccessFile file;
	private FileChannel fchannel;
	private int mextCount;
	private long dataOffset;
	
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

		double appx = (file.length() / DATA_BLOCK_SIZE);
		this.mextCount = ((appx % (double)METADATA_EXTENT_SIZE) == 0) ?
			(int)Math.ceil(appx) :
			(int)appx;

		this.mextCount /= METADATA_EXTENT_SIZE;
		if(this.mextCount == 0)
			++mextCount;
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
				   exaddr,
				   exaddr + METADATA_EXTENT_SIZE);

		ext = new MetadataExtent(mbuf, exn);
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

	public void format()
		throws IOException
	{
		for(int mext=0; mext < getMetadataExtentCount(); ++mext)
			getMetadataExtent(mext).erase();
	}

	public static void main(String[] args)
		throws Exception
	{
		HrfsDisk disk;

		disk = new HrfsDisk(Paths.get("test"));
		disk.format();

		System.out.println("yo");
		System.out.println(Paths.get("."));
		System.out.println(disk.getMetadataExtentCount());
	}
}
