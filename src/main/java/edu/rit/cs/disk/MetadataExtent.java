package edu.rit.cs.disk;

import java.nio.MappedByteBuffer;
import java.util.LinkedList;

class MetadataExtent
{
	private LinkedList<MetadataBlock> mblocks;
	private MappedByteBuffer mbuf;
	private long exn;

	/**
	 * In memory object for mapped region, this is for management
	 * of the objects within the extent, including put and delete.
	 * @param buf Mapped buffer region.
	 * @param exn Extent number on disk
	 */
	public MetadataExtent(MappedByteBuffer mbufp, int exn)
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
			mblocks.add(new MetadataBlock(mbuf, mblk));
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
		for(MetadataBlock mblock : this.mblocks)
		{
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
