/**
 * Copyright © 2015
 * Hrfs Data Block
 *
 * A fully qualified data block object for hrfs, this is the in memory reference
 * object for on disk data. A valid DataBlock contains a hash that refers to the
 * hash sum of the data, and all operations should be considered immutable.
 *
 * @file DataBlock.java
 * @author Will Dignazio <wdignazio@gmail.com>
 */
package edu.rit.cs;

import java.util.Arrays;

public final class DataBlock
	implements Block
{
	private final long   index;
	private final byte[] buffer;
	private final byte[] hashval;

	/** Hide the default constructor */
	private DataBlock()
	{
		index = -1;
		buffer = null;
		hashval = null;
	}

	/**
	 * Build a new DataBlock that contains a hash value, and a data
	 * buffer. The block for now supports an index field that indicates where
	 * it lies in a contiguous data structure.
	 * @param buf Backing byte buffer for the block
	 * @param hval Hash value in byte array for the block
	 */
	public DataBlock(byte[] buf, byte[] hval, long idx)
		throws IllegalArgumentException
	{
		if(buf == null || hval == null)
			throw new IllegalArgumentException("Invalid block buffers");
		if(idx < 0)
			throw new IllegalArgumentException("Block index must be >= 0");
		
		buffer = buf;
		hashval = hval;
		index = idx;
	}

	/** Return size of block data buffer */
	@Override
	public long length()
	{ return buffer.length; }

	/** Return index of block within file */
	@Override
	public long index()
	{ return index; }

	/** 
	 * Return a reference to the underlying data buffer.
	 */
	@Override
	public byte[] data()
	{
		return buffer;
	}

	/**
	 * Return a reference to the underlying hash value buffer.
	 */
	public byte[] hash()
	{
		return hashval;
	}
}
