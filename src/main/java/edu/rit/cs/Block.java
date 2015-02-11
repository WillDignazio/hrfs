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

interface Block
{
	/**
	 * Gets the size of the block in bytes, this is expected to be a safe
	 * value that can be seeked to by the blocks backing byte buffer.
	 * @return long Size in bytes.
	 */
	public long length();
}
