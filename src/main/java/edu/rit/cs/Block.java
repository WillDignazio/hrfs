/**
 * Copyright © 2014
 * Hrfs Block Interface
 *
 * The Block interface allows hrfs to generically move around blocks of data,
 * of any size and type. Care should be taken not to confuse the types, but this
 * allows portability of block factories and various disk subsystems.
 *
 * @file Block.java
 * @author Will Dignazio <wdignazio@gmail.com>
 */
package edu.rit.cs;

public interface Block
{
	/**
	 * Gets the size of the block in bytes, this is expected to be a safe
	 * value that can be seeked to by the blocks backing byte buffer.
	 * @return long Size in bytes.
	 */
	public long length();

	/**
	 * Gets the index of the block on disk, in length size quantities.
	 * @return Index of block.
	 */
	public long index();

	/**
	 * Get a reference to the underlying data of this block. This will
	 * be a direct reference, and modifications to this will change the
	 * block data.
	 * @return Reference to block data.
	 */
	public byte[] data();
}
