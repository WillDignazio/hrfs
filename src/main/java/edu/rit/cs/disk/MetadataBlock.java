package edu.rit.cs.disk;

import java.util.Arrays;
import java.nio.MappedByteBuffer;
import java.nio.ByteBuffer;

class MetadataBlock
{
	private MappedByteBuffer mbuf;
	private int mxn;
	private int offset;

	/**
	 * In memory object for metadata block of mapped region, when
	 * given the number within the extent, this will calculate the
	 * offsets for the set and get methods.
	 * @param mbuf Parent byte buffer
	 * @param mxn Metadata block number
	 */
	public MetadataBlock(MappedByteBuffer mbuf, int mxn)
	{
		this.mbuf = mbuf;
		this.mxn = mxn;
		this.offset = mxn * HrfsDisk.METADATA_BLOCK_SIZE;
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
		byte[] cregion;

		cregion = Arrays.copyOfRange(this.mbuf.array(),
					     offset,
					     offset +  HrfsDisk.METADATA_BLOCK_SIZE);
		return cregion;
	}

	/**
	 * Set the key for the metadata block.
	 * @param key Key for metadata block
	 */
	public boolean setKey(byte[] key)
	{
		this.mbuf.put(key,
			      0,
			      HrfsDisk.METADATA_KEY_SIZE);
		return true;
	}

	/**
	 * Set the location pointer to the specified value.
	 * @param loc Location on disk.
	 */
	public void setDataLocation(long loc)
	{
		ByteBuffer buffer;
		byte[] odata;

		buffer = ByteBuffer.allocate(Long.SIZE / Byte.SIZE);
		odata = buffer.putLong(loc).array();

		System.out.println(odata.length);
		this.mbuf.put(odata,
			      HrfsDisk.METADATA_KEY_SIZE+1,
			      odata.length);
	}
}
