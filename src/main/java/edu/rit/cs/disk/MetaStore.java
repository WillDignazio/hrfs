/**
 * Copyright © 2015
 * Hadoop Replicating File System
 *
 * Sub-block store that is used to store metadata blocks within the filesystem.
 * 
 * @file MetaStore.java
 * @author Will Dignazio <wdignazio@gmail.com>
 */
package edu.rit.cs.disk;

import org.apache.commons.lang.StringUtils;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.io.RandomAccessFile;
import java.io.FileNotFoundException;
import java.io.IOException;

class MetaStore
	implements HrfsBlockStore
{
	public static final int METADATA_EXTENT_SIZE	= 4096;
	public static final int METADATA_KEY_SIZE	= 20;
	public static final int METADATA_BLOCK_SIZE	= 64;

	private Path mPath;
	private RandomAccessFile mFile;
	private FileChannel mChannel;
	private long rootBlkAddr;
	private long mextCount;

	/** Must use a file */
	private MetaStore() { }
	
	/**
	 * Open a file for use with this MetaStore, does not have to be a valid
	 * metadata storage device.
	 * @param path Path to metastorage device/file
	 */
	public MetaStore(Path path)
		throws FileNotFoundException, IOException
	{
		this.mPath = path;

		if(Files.notExists(mPath, LinkOption.NOFOLLOW_LINKS))
			throw new FileNotFoundException(mPath.toString());

		this.mFile = new RandomAccessFile(mPath.toFile(), "rw");
		this.mChannel = mFile.getChannel();
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
		mbuf = mChannel.map(FileChannel.MapMode.READ_WRITE,
				    0,
				    exaddr + METADATA_EXTENT_SIZE).load();

		ext = new MetadataExtent(mbuf, exn);
		return ext;
	}
	
	/**
	 * Formats the metadata storage unit.
	 */
	@Override
	public void format()
		throws IOException
	{
		SuperBlock sb;
		long appxMext;
		long mblkCount;

		/*
		 * Give a low-ball approximate count of metadata blocks that we
		 * can fit into this metadata storage unit.
		 */
		System.out.println("mFile.length: " + mFile.length());
		appxMext = mFile.length() / METADATA_EXTENT_SIZE;

		System.out.println("Formatting " + mPath.toString() + ":");
		for(int mext=0; mext < appxMext; ++mext) {
			double pc = ((double)mext / appxMext) * 100.0;
			System.out.print("[");
			System.out.print(StringUtils.repeat("#", (int)pc));

			/* 
			 * Besides the pretty printing we're going to erase all
			 * of the extents on disk.
			 */
			getMetadataExtent(mext).erase();

			System.out.print(StringUtils.repeat( " ", 99 - (int)pc) + "] ");
			System.out.print("%" + (int)Math.ceil(pc) + "\r");
		}
		System.out.println();

		/* For immediate use, set the new mext count */
		this.mextCount = appxMext;
		mblkCount = (mextCount * METADATA_EXTENT_SIZE) /
			METADATA_BLOCK_SIZE;

		sb = getSuperBlock();
		sb.setRootBlockAddress(0);
		sb.setMetadataBlockCount(this.mextCount);
		sb.setMetadataBlockAvailable(mblkCount);
		sb.setMagic(SuperBlock.SUPER_MAGIC);
	}

	/**
	 * Insert a metadata object into the metadata storage.
	 * @param key Key for the storage unit.
	 * @param data Data for metadata block
	 * @return whether the object was stored on disk
	 */
	@Override
	public boolean insert(byte[] key, byte[] data)
		throws IOException
	{
		return false;
	}

	/**
	 * Get a metadata object value from the metadata storage.
	 * @param key Key for metadata object.
	 * @return val Value for metadata object.
	 */
	@Override
	public byte[] get(byte[] key)
		throws IOException
	{
		return new byte[1];
	}

	/**
	 * Remove a metadata block object from storage.
	 * @param key Key for block to remove
	 * @return Whether block was removed.
	 */
	@Override
	public boolean remove(byte[] key)
		throws IOException
	{
		return false;
	}

	/**
	 * Get the number of extents that are present in this storage.
	 * @return number of extents.
	 */
	public long getBlockCount()
	{
		System.out.println("metastore mextcount: " + mextCount);
		return (mextCount * METADATA_EXTENT_SIZE) / METADATA_BLOCK_SIZE;
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

		mbuf = mChannel.map(FileChannel.MapMode.READ_WRITE,
				    0,
				    SuperBlock.SUPERBLOCK_SIZE).load();

		sblock = new SuperBlock(mbuf);
		return sblock;
	}
}
