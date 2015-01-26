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

import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.concurrent.ConcurrentUtils;
import java.util.concurrent.Future;
import java.util.Arrays;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.Files;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.io.RandomAccessFile;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;

class MetaStore
	extends BlockStore<MetadataBlock>
{
	public static final int METADATA_BLOCK_SIZE = 64;
	public static final int METADATA_EXTENT_SIZE = 4096;
	public static final int METADATA_KEY_SIZE = 20;
	private static final byte[] METADATA_NULL_KEY = new byte[METADATA_KEY_SIZE];
	
	private SuperBlock sb;

	/*
	 * XXX Temporary Fields.
	 *
	 * These are fields that are used temporarily before we create a safer,
	 * more journal like method of inserting and removing blocks. These are
	 * used in lieu of constantly writing to the superblock the new
	 * information.
	 */
	private long _mext_idx;		// Metadata Extent Allocation Index
	private long _dblk_idx;		// Data Block Allocation Index

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
		super(path);

		this.sb = getSuperBlock();
		this._mext_idx = sb.getMetadataBlockIndex();
		this._dblk_idx = sb.getDataBlockIndex();
	}

	/**
	 * Retrieve a mapping to the numeric extent given to the method.
	 * @param exn Extent number
	 * @return mbuf ByteBuffer of extent
	 */
	private MetadataExtent getMetadataExtent(long exn)
		throws IOException
	{
		MetadataExtent ext;
		ByteBuffer mbuf;
		long exaddr;

		exaddr = METADATA_EXTENT_SIZE * exn;
		mbuf = this.getChannel().map(FileChannel.MapMode.READ_WRITE,
					     SuperBlock.SUPERBLOCK_SIZE,
					     exaddr + METADATA_EXTENT_SIZE).load();

		ext = new MetadataExtent(mbuf, exn);
		return ext;
	}

	/**
	 * Retrieve the metadata block as an index from disk.
	 * @param bxn Block index number
	 */
	private MetadataBlock getMetadataBlock(long bxn)
		throws IOException
	{
		MetadataBlock mblk;
		MetadataExtent ext;
		List<MetadataBlock> extblks;
		long byteoff;
		long mextn;
		int rblkn;

		/* Translate to extent block belongs to. */
		byteoff = (bxn * METADATA_BLOCK_SIZE);
		
		mextn = byteoff / METADATA_EXTENT_SIZE;
		ext = getMetadataExtent(mextn);
		extblks = ext.getMetadataBlocks();

		/* Get the relative block number */
		rblkn = (int)(byteoff % METADATA_EXTENT_SIZE);

		return ext.getMetadataBlocks().get(rblkn);
	}

	/**
	 * Formats the metadata storage unit.
	 */
	@Override
	public synchronized void format()
		throws IOException
	{
		long appxMext;
		long mextCount;
		long mblkCount;

		/*
		 * Give a low-ball approximate count of metadata blocks that we
		 * can fit into this metadata storage unit.
		 */
		appxMext = this.size() / METADATA_EXTENT_SIZE;

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
		mextCount = appxMext;
		mblkCount = (mextCount * METADATA_EXTENT_SIZE) /
			METADATA_BLOCK_SIZE;

		sb.setDataBlockIndex(0);
		sb.setMetadataBlockIndex(0);
		sb.setMetadataBlockCount(mblkCount);
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
	public synchronized Future<MetadataBlock> insert(byte[] key, byte[] data)
		throws IOException
	{
		MetadataBlock mblk;
		ByteBuffer locbuf;
		long blkidx;
		long blkaddr;

		if(isClosed() == true)
			throw new IOException("MetaStore is closed");

		if(key.length != METADATA_KEY_SIZE)
			throw new IllegalArgumentException("Invalid Key Size");
		if(data.length != HrfsDisk.LONGSZ) // Pointer size for address
			throw new IllegalArgumentException("Invalid Block Size");

		/* Check if first node */
		System.out.println("_mext_idx: " + _mext_idx);
		blkidx = _mext_idx;
		mblk = getMetadataBlock(blkidx);

		if(blkidx == 0) {
			System.out.println("Inserting root mblock");
			if(Arrays.equals(mblk.getKey(), METADATA_NULL_KEY) == false)
				throw new IOException("Root object filled, _mextidx == 0");

			/* Set new block attributes */
			locbuf = ByteBuffer.wrap(data);
			blkaddr = locbuf.getLong();
			mblk.setDataBlockLocation(blkaddr);
			mblk.setKey(key);

			/* XXX Serialized for now */
			return ConcurrentUtils.constantFuture(mblk);
		}

		return null;
	}

	/**
	 * Get a metadata object value from the metadata storage.
	 * @param key Key for metadata object.
	 * @return val Value for metadata object.
	 */
	@Override
	public Future<MetadataBlock> get(byte[] key)
		throws IOException
	{ throw new NotImplementedException(); }


	/**
	 * Remove a metadata block object from storage.
	 * @param key Key for block to remove
	 * @return Whether block was removed.
	 */
	@Override
	public Future<Boolean> remove(byte[] key)
		throws IOException
	{ throw new NotImplementedException(); }


	/**
	 * Gets the superblock of the on disk structure.
	 * @return sblock Superblock of the on disk structure.
	 */
	private SuperBlock getSuperBlock()
		throws IOException
	{
		ByteBuffer mbuf;
		SuperBlock sblock;

		mbuf = this.getChannel().map(FileChannel.MapMode.READ_WRITE, 0,
					     SuperBlock.SUPERBLOCK_SIZE).load();

		sblock = new SuperBlock(mbuf);
		return sblock;
	}
}

