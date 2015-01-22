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
	private LinkedList<MetadataBlock> blocks;
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
		this.blocks = new LinkedList<MetadataBlock>();

		for(int mblk=0;
		    mblk < (MetaStore.METADATA_EXTENT_SIZE / MetaStore.METADATA_BLOCK_SIZE);
		    mblk++) {
			MetadataBlock blk;

			blk = new MetadataBlock(mbuf.duplicate(), mblk);
			blocks.add(blk);
		}
	}

	/**
	 * Erase the contents of this metadata extent, this is done
	 * by filling each value of the metadata blocks to 0.
	 */
	public void erase()
	{
		byte[] zbuf;

		zbuf = new byte[MetaStore.METADATA_KEY_SIZE];
		
		/*
		 * XXX Warning
		 * This does the 'quick' format of the metadata
		 * blocks. We assume that a block can be allocated
		 * if the key is zero'd and that the other values
		 * within it are invalid.
		 */
		for(MetadataBlock mblock : this.blocks) {
			mblock.setKey(zbuf);
			mblock.setDataBlockLocation(0);
			mblock.setNextBlockLocation(0);
			mblock.setLeftBlockLocation(0);
			mblock.setRightBlockLocation(0);
		}
	}

	/**
	 * Return the metadata blocks in this extent.
	 * @return mblocks Metadata Blocks list
	 */
	public LinkedList<MetadataBlock> getMetadataBlocks()
	{
		return this.blocks;
	}
}
