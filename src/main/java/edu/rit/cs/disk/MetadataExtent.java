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
	private LinkedList<MetadataBlock> mblocks;
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

		mblocks = new LinkedList<MetadataBlock>();
		for(int mblk=0;
		    mblk < (HrfsDisk.METADATA_EXTENT_SIZE / HrfsDisk.METADATA_BLOCK_SIZE);
		    mblk++) {
			/* 
			 * Create the metadata block in memory representations,
			 * provide them the buffer so they have direct access to
			 * the mapped memorry.
			 */
			mblocks.add(new MetadataBlock(mbuf.duplicate(), mblk));
		}
	}

	/**
	 * Erase the contents of this metadata extent, this is done
	 * by filling each value of the metadata blocks to 0.
	 */
	public void erase()
	{
		byte[] zbuf;

		zbuf = new byte[HrfsDisk.METADATA_KEY_SIZE];
		for(MetadataBlock mblock : this.mblocks) {
			mblock.setKey(zbuf);
			mblock.setDataLocation(0);
		}
	}

	/**
	 * Return the metadata blocks in this extent.
	 * @return mblocks Metadata Blocks list
	 */
	public LinkedList<MetadataBlock> getMetadataBlocks()
	{
		return this.mblocks;
	}
}
