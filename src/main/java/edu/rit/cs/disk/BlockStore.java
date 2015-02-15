/**
 * Copyright © 2014
 * Hrfs Block Store API
 *
 * Defines what a block storage device for Hrfs should support. This will be
 * used by higher API's to store generic blocks of data to disk. Implementors
 * of this API may or may not specify the type of block they are using.
 *
 * @file BlockFactory.java
 * @author Will Dignazio <wdignazio@gmail.com>
 */
package edu.rit.cs.disk;

public interface BlockStore
{
	/**
	 * Create a new BlockStore, using the underlying hrfs configuration as
	 * the parameters for the location and specifications for the block
	 * storage unit.
	 * @return Whether the creation of the block store was successful.
	 */
	public boolean create();
}
