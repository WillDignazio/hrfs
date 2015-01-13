/**
 * Copyright © 2015
 * Hadoop Replicating File System
 *
 * A MetadataExtent is an in memory reference to a memory map of
 * a buffer that contains metadata information. This can be broken
 * down into blocks that can be used to modify the exact information
 * on disk.
 */
package edu.rit.cs.disk;

import java.nio.ByteBuffer;
import java.util.LinkedList;

class MetadataExtent
{
	private LinkedList<MetadataBlock> freeBlocks;
	private LinkedList<MetadataBlock> usedBlocks;
	private ByteBuffer mbuf;
	private long exn;

	/**
	 * In memory object for mapped region, this is for management
	 * of the objects within the extent, including put and delete.
	 * @param buf Mapped buffer region.
	 * @param exn Extent number on disk
	 */
	public MetadataExtent(ByteBuffer mbuf, int exn)
	{
		this.mbuf = mbuf;
		this.exn = exn;

		freeBlocks = new LinkedList<MetadataBlock>();
		usedBlocks = new LinkedList<MetadataBlock>();

		for(int mblk=0;
		    mblk < (HrfsDisk.METADATA_EXTENT_SIZE / HrfsDisk.METADATA_BLOCK_SIZE);
		    mblk++) {
			MetadataBlock blk;

			blk = new MetadataBlock(mbuf.duplicate(), mblk);
			if(blk.isAllocated())
				usedBlocks.add(blk);
			else
				freeBlocks.add(blk);
		}
	}

	/**
	 * Allocate a block from within the extent, this will reduce the
	 * of free blocks by 1, and increase the amount of used blocks
	 * by 1. If there are no more blocks within the extent, null shall
	 * be returned.
	 *
	 * XXX Need to throw out of blocks exceptions
	 * @return block New metadata block
	 */
	public MetadataBlock allocateMetadataBlock()
	{
		MetadataBlock block;
		byte[] nbuf;

		if(usedBlocks.size() == 0)
			return null;

		/* Mark it as a dummy value */
		nbuf = new byte[HrfsDisk.METADATA_KEY_SIZE];
		nbuf[nbuf.length-1] = 0xF;

		block = usedBlocks.remove(0);
		block.setKey(nbuf);

		return block;
	}

	/**
	 * Get the number of free blocks in the extent.
	 * @return blocks Number of free metadata blocks
	 */
	public int freeBlockCount()
	{
		return this.freeBlocks.size();
	}

	/**
	 * Get the number of used blocks in the extent.
	 * @return used Number of used metadata blocks.
	 */
	public int usedBlockCount()
	{
		return this.usedBlocks.size();
	}

	/**
	 * Erase the contents of this metadata extent, this is done
	 * by filling each value of the metadata blocks to 0.
	 */
	public void erase()
	{
		byte[] zbuf;

		zbuf = new byte[HrfsDisk.METADATA_KEY_SIZE];
		
		/*
		 * XXX Warning
		 * This does the 'quick' format of the metadata
		 * blocks. We assume that a block can be allocated
		 * if the key is zero'd and that the other values
		 * within it are invalid.
		 */
		for(MetadataBlock mblock : this.usedBlocks)
			mblock.setKey(zbuf);
	}

	/**
	 * Return the metadata blocks in this extent.
	 * @return mblocks Metadata Blocks list
	 */
	public LinkedList<MetadataBlock> getMetadataBlocks()
	{
		return this.usedBlocks;
	}
}
