/**
 * Copyright © 2015
 * Hadoop Replicating File System
 *
 * Abstract class that defines what a block looks like within a storage unit.
 *
 * @file Block.java
 * @author Will Dignazio <wdignazio@gmail.com>
 */
package edu.rit.cs.disk;

import java.nio.ByteBuffer;

abstract class Block
{
	private final ByteBuffer _buffer;

	private Block()
	{
		_buffer = null;
	}
	
	/**
	 * Build a block object of a defined size and buffer.
	 * @param size Size in bytes
	 * @param buffer Backing buffer for block
	 */
	public Block(ByteBuffer buffer)
	{
		this._buffer = buffer;
	}

	/**
	 * Returns the size of the block in bytes.
	 * @return nbytes Size of block in bytes.
	 */
	public abstract long size();

	/**
	 * Returns the backing buffer of this block, to keep other folks from
	 * accessing the internal byte buffer when a Block is returned, keep
	 * this method protected.
	 * @return buffer Backing buffer of block
	 */
	protected ByteBuffer getBuffer() { return _buffer; }
}
