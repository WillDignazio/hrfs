/**
 * Copyright © 2015
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

import edu.rit.cs.DataBlock;
import java.io.IOException;

public interface BlockStore
{
	/**
	 * Create a new BlockStore, using the underlying hrfs configuration as
	 * the parameters for the location and specifications for the block
	 * storage unit.
	 * @return Whether the creation of the block store was successful.
	 */
	public boolean create()
		throws IOException;

	/**
	 * Open up an instance of a block store, this insinuiates that a block
	 * store may not immediately be open for read or writing data.
	 */
	public boolean open()
		throws IOException;

	/**
	 * Similarly determine whether the block store is open for writing.
	 */
	public boolean isOpen()
		throws IOException;

	/**
	 * Implementation of insert for the BlockStore unit, this will return
	 * whether the specified insertion was successful. The gaurantees about
	 * whether the data is immediately retrievable are dependent on the
	 * implementation.
	 */
	public boolean insert(DataBlock blk)
		throws IOException;
}
