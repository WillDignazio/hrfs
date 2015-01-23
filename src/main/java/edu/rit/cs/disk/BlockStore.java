/**
 * Copyright © 2015
 * Hadoop Replicating File System
 *
 * Abstract class that allows a running hrfs instance to store blocks on disk,
 * and likewise retrieve them.
 *
 * @file BlockStore.java
 * @author Will Dignazio <wdignazio@gmail.com>
 */
package edu.rit.cs.disk;

import java.util.concurrent.Future;
import java.nio.channels.FileChannel;
import java.nio.file.LinkOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

public abstract class BlockStore<T extends Block>
{
	private RandomAccessFile _file;
	private FileChannel _channel;
	private Path _path;
	private long _size;
	
	/**
	 * Produce a default store with uninitialized values.
	 */
	public BlockStore()
	{
		this._path = null;
		this._file = null;
		this._channel = null;
		this._size = 0;
	}

	/**
	 * Construct a base block store object, opening the file path
	 * as a given base for the backing storage.
	 * @param path Backing storage device for store.
	 */
	public BlockStore(Path path)
		throws FileNotFoundException, IOException
	{
		if(Files.notExists(path, LinkOption.NOFOLLOW_LINKS))
			throw new FileNotFoundException(path.toString());

		this._path = path;
		this._file = new RandomAccessFile(_path.toFile(), "rw");
		this._channel = _file.getChannel();
		this._size = _file.length();
	}

	/**
	 * Format the block storage such that no data appears to be in it.
	 * Implementors of this function shall not allow blocks previous to 
	 * format to be retrieved.
	 */
	public abstract void format()
		throws IOException;

	/**
	 * Insert a block of data into the storage unit.
	 * @param key Block key
	 * @param blk Block of data
	 * @return success Whether insertion was successful
	 */
	public abstract Future<T> insert(byte[] key, byte[] blk)
		throws IOException;

	/**
	 * Get a block of data from the storage unit.
	 * @param key Key for block
	 * @return data Block of data
	 */
	public abstract Future<T> get(byte[] key)
		throws IOException;

	/**
	 * Remove a block of data from the storage unit.
	 * @param key Block key
	 * @return Whether removal was successful
	 */
	public abstract Future<Boolean> remove(byte[] key)
		throws IOException;

	/**
	 * Gets the channel to the backing file or device of this store.
	 * @return channel Channel to file or device.
	 */
	protected FileChannel getChannel() { return this._channel; }

	/**
	 * Gets the size in bytes of this block storage.
	 * @return size Size in bytes.
	 */
	public long size() { return this._size; }
}
