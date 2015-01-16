/**
 * Copyright © 2015
 * Hadoop Replicating File System
 *
 * The in memory representation of the on disk superblock,
 * this block contains the top level node for the on disk
 * tree structure.
 *
 * @file SuperBlock.java
 * @author Will Dignazio <wdignazio@gmail.com>
 */
package edu.rit.cs.disk;

import java.nio.ByteBuffer;

class SuperBlock
{
	private static final int BOOTSECTOR_SIZE = 512;
	private static final int ROOTBLOCK_OFFSET = BOOTSECTOR_SIZE;
	public static final int SUPER_MAGIC = 0xCAFEBABE;

	private ByteBuffer mbuf;

	public SuperBlock(ByteBuffer mbuf)
	{
		this.mbuf = mbuf;
	}

	/**
	 * Check the magic number for the superblock, if this fails,
	 * the superblock has become invalid.
	 * @return valid True for valid, False for invalid
	 */
	public boolean isValid()
	{
		long magic;

		magic = this.mbuf.getLong(HrfsDisk.SUPERBLOCK_SIZE -
					  (Long.SIZE / Byte.SIZE));

		if(magic == SUPER_MAGIC)
			return true;

		return false;
	}

	/**
	 * Gets the root metadata block address from the superblock, this is
	 * the first key of the node in the on disk b+ tree structure.
	 */
	public long getRootBlockAddress()
	{
		long addr;

		addr = this.mbuf.getLong(ROOTBLOCK_OFFSET);
		return addr;
	}

	/**
	 * Sets the root metadata block address of the superblock.
	 * @param addr Address of the root metadata block.
	 */
	public void setRootBlockAddress(long addr)
	{
		this.mbuf.putLong(ROOTBLOCK_OFFSET, addr);
	}
	
	/**
	 * Set the superblock magic value, this will make it such
	 * that this superblock will pass a validity test.
	 * @param m Magic value
	 */
	public void setMagic(long m)
	{
		this.mbuf.putLong(HrfsDisk.SUPERBLOCK_SIZE -
				  (Long.SIZE / Byte.SIZE),
				  m);
	}

	/**
	 * Erase the contents of the superblock, fields that
	 * would lead to a valid filesystem.
	 */
	public void erase()
	{
		/* XXX For now just zero the thing */
		for(int b=0; b < HrfsDisk.SUPERBLOCK_SIZE; ++b)
			this.mbuf.put(b, (byte)0);
	}
}
