/**
 * Copyright © 2015
 * Hadoop Replicating File System
 *
 * Smallest metadata object, a block within a metadata extent, is
 * an in memory representation of the mapped memory on disk. This
 * is used to provide an interface to metadata values, and information
 * about where data lies.
 *
 * @file MetadataBlock.java
 * @author William Dignazio <wdignazio@gmail.com>
 */
package edu.rit.cs.disk;

import com.google.common.primitives.UnsignedBytes;
import org.apache.hadoop.io.Writable;
import java.util.Comparator;
import java.util.Arrays;
import java.nio.ByteBuffer;
import java.io.IOException;
import java.io.DataOutput;
import java.io.DataInput;

final class MetadataBlock
	extends Block
	implements Comparable<MetadataBlock>
{
	private static final int KEY_OFFSET = 0;
	private static final int LOCATION_OFFSET = MetaStore.METADATA_KEY_SIZE;
	private static final int NEXT_OFFSET = LOCATION_OFFSET + (Long.SIZE / Byte.SIZE);
	private static final int LEFT_OFFSET = NEXT_OFFSET + (Long.SIZE / Byte.SIZE);
	private static final int RIGHT_OFFSET = LEFT_OFFSET + (Long.SIZE / Byte.SIZE);

	private int bxn;
	private int offset;
	private MetadataExtent parent;

	public static class BlockComparator
		implements Comparator<MetadataBlock>
	{
		@Override
		public int compare(MetadataBlock b1, MetadataBlock b2)
		{
			/* XXX Ugly hack wrapper */
			Comparator<byte[]> comparator;
			comparator = UnsignedBytes.lexicographicalComparator();
			return comparator.compare(b1.getKey(), b2.getKey());
		}

		@Override
		public boolean equals(Object obj)
		{
			if(obj instanceof MetadataBlock)
				return true;

			return false;
		}
	}

	/**
	 * In memory object for metadata block of mapped region, when
	 * given the number within the extent, this will calculate the
	 * offsets for the set and get methods.
	 * @param mext Parent metadata extent
	 * @param bxn Metadata block number within extent
	 */
	public MetadataBlock(MetadataExtent mext, int bxn)
	{
		super(mext.copyBuffer().duplicate());

		this.parent = mext;
		this.bxn = bxn;
		this.offset = bxn * MetaStore.METADATA_BLOCK_SIZE;
	}

	/**
	 * Get a comparator for metadata blocks.
	 * @return Static comparator reference.
	 */
	public static BlockComparator getComparator()
	{
		return new BlockComparator();
	}
	
	/**
	 * Returns the size of this block in bytes.
	 * @return nbytes Block size in bytes
	 */
	@Override
	public long size()
	{
		return MetaStore.METADATA_BLOCK_SIZE;
	}
	
	/**
	 * If the metadata block has a non-zero'd key, we consider it
	 * to be unallocated, or unused.
	 * @return allocated Whether the block has been allocated.
	 */
	public boolean isAllocated()
	{
		for(int b=0; b < MetaStore.METADATA_KEY_SIZE; ++b)
			if(this.getBuffer().get(b) != 0)
				return true;

		return false;
	}

	/**
	 * Get the key for the metadata block, this is used to navigate
	 * the tree, and determine the location of a block of data.
	 * 
	 * This returns a *copy* of the value found on disk from this node.
	 * @return key Key of this metadata block
	 */
	public byte[] getKey()
	{
		ByteBuffer ebuf;
		byte[] cregion;

		cregion = new byte[MetaStore.METADATA_KEY_SIZE];

		/* XXX Needs to be optimized */
		for(int k=0; k < MetaStore.METADATA_KEY_SIZE; ++k) {
			cregion[k] = getBuffer().get(offset + KEY_OFFSET + k);
			if(cregion[k] != 0)
				System.out.println("Nonzero on the " + k +"th value");
		}

		return cregion;
	}

	/**
	 * Set the key for the metadata block.
	 * @param key Key for metadata block
	 */
	public void setKey(byte[] key)
	{
		for(int b=0; b < MetaStore.METADATA_KEY_SIZE; ++b)
			this.getBuffer().put(offset + KEY_OFFSET+b, key[b]);
	}

	/**
	 * Set the location pointer to the specified value.
	 * @param locptr Location on disk.
	 */
	public void setDataBlockLocation(long locptr)
	{
		this.getBuffer().putLong(offset + LOCATION_OFFSET, locptr);
	}

	/**
	 * Get the location pointer to the specified value.
	 * @return locptr Pointer to location within disk
	 */
	public long getDataBlockLocation()
	{
		return this.getBuffer().getLong(offset + LOCATION_OFFSET);
	}

	/**
	 * Set the next pointer for a metadata block on disk, this must
	 * be the location of the first byte of the next metadata block.
	 * @param ptr Location on disk
	 */
	public void setNextBlockLocation(long ptr)
	{
		this.getBuffer().putLong(offset + NEXT_OFFSET, ptr);
	}

	/**
	 * Get the next pointer for a metadata block on disk, this must
	 * be translated into a metadata block for use.
	 * @return ptr Location disk.
	 */
	public long getNextBlockLocation()
	{
		return this.getBuffer().getLong(offset + NEXT_OFFSET);
	}

	/**
	 * Set the left pointer for a metadata block on disk, this must be
	 * the location of the first byte of the left metadata block.
	 * @param ptr Location on disk
	 */
	public void setLeftBlockLocation(long ptr)
	{
		this.getBuffer().putLong(offset + LEFT_OFFSET, ptr);
	}

	/**
	 * Get the left pointer for a metadata block on disk, this must
	 * be translated into a metadata block for use.
	 * @return ptr Location on disk.
	 */
	public long getLeftBlockLocation()
	{
		return this.getBuffer().getLong(offset + LEFT_OFFSET);
	}

	/**
	 * Set the right pointer for a metadata block on disk, this must be
	 * the location of the first byte of the right metadata block.
	 * @param ptr Location on disk
	 */
	public void setRightBlockLocation(long ptr)
	{
		this.getBuffer().putLong(offset + RIGHT_OFFSET, ptr);
	}
	
	/**
	 * Get the right pointer for a metadata block on disk, this must
	 * must be translated into a metadata block for use.
	 * @return ptr Location on disk.
	 */
	public long getRightBlockLocation()
	{
		return this.getBuffer().getLong(offset + RIGHT_OFFSET);
	}

	/**
	 * Get the extent that this block was create from.
	 * @return mext Parent metadata extent
	 */
	public MetadataExtent getParentExtent()
	{
		return this.parent;
	}

	/**
	 * Comparison method for the metadata block, uses the entirety of the
	 * key value to determine it's order compared to another block.
	 * @param blk Metadata block to compare to
	 * @return cmp <|>|= 0 Comparison of blocks
	 */
	@Override
	public int compareTo(MetadataBlock blk)
	{
		Comparator<byte[]> comparator;

		comparator = UnsignedBytes.lexicographicalComparator();
		return comparator.compare(this.getKey(), blk.getKey());
	}
}
