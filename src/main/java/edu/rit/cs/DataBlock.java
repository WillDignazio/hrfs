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
	 * NOTE: The buffer and hval given to DataBlock will be COPIED during
	 * runtime. This is currently one through the given Arrays.copyOf method,
	 * but is subject to change in the future. This is to ensure immutability
	 * of the block.
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
		
		buffer = Arrays.copyOf(buf, buf.length);
		hashval = Arrays.copyOf(hval, hval.length);
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
	 * NOTE: Return a COPY of the underlying block buffer.
	 * XXX: Likely to change in future commits
	 */
	@Override
	public byte[] data()
	{
		return Arrays.copyOf(buffer, buffer.length);
	}

	/**
	 * Return a COPY of the underlying hash value buffer.
	 */
	public byte[] hash()
	{
		return Arrays.copyOf(hashval, hashval.length);
	}
}
