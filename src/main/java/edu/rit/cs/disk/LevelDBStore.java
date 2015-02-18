/**
 * Copyright © 2014
 * Hrfs LevelDB Block Store
 *
 * An implementation of a BlockStore that is backed using LevelDB.
 * By implementing the BlockStore API, higher layer can create, destroy, put,
 * remove, and check if blocks exist.
 *
 * @file LevelDBStore.java
 * @author Will Dignazio <wdignazio@gmail.com>
 */
package edu.rit.cs.disk;

import static org.iq80.leveldb.impl.Iq80DBFactory.*;
import org.iq80.leveldb.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.*;
import java.io.*;

import edu.rit.cs.HrfsConfiguration;
import edu.rit.cs.DataBlock;
import edu.rit.cs.HrfsKeys;

class LevelDBStore
	implements BlockStore
{
	static
	{
		HrfsConfiguration.init();
	}

	private static final Log LOG = LogFactory.getLog(LevelDBStore.class);

	private HrfsConfiguration conf;
	private String storePath;
	private Options options;
	private boolean isopen;
	private File lvlfd;
	private DB lvldb;

	/**
	 * Construct a LevelDB Block Store instance, this will not create or open
	 * store outright, but instead give a handle to a store as configured in
	 * the site conf.
	 * Use the BlockStore API to open, create, or use a block store.
	 */
	public LevelDBStore(String path)
		throws IOException
	{
		String nodepath;

		storePath = path;
		if(storePath == null)
			storePath = conf.get(HrfsKeys.HRFS_NODE_STORE_PATH);

		if(storePath == null)
			throw new IOException("Store Path unset, refusing to construct store.");		

		options = new Options();
		options.compressionType(CompressionType.NONE);

		lvlfd = new File(storePath);
		if(lvlfd.exists() && (!lvlfd.canWrite() || !lvlfd.canRead()))
			throw new IOException("Insufficient Permission");		
	}

	/**
	 * Return whether the LevelDB store is open.
	 * @return Whether the store is open.
	 */
	@Override
	public boolean isOpen()
	{ return isopen; }		
	
	/**
	 * Open The block store, allowing it to be used for block storage.
	 */
	@Override
	public boolean open()
		throws IOException
	{
		
		lvldb = factory.open(lvlfd, options);
		if(lvldb == null)
			return false;

		isopen = true;
		return true;
	}
	/**
	 * Create a new LevelDB instance, if the location does not exist, and
	 * the permissions are correct, a new leveldb instance will be generated.
	 * This method also calls open on the data store, and is immediately
	 * available for use.
	 */
	@Override
	public boolean create()
		throws IOException
	{
		/* By default, create if possible */
		options.createIfMissing(true);		
		lvldb = factory.open(lvlfd, options);
		if(lvldb == null)
			throw new IOException("Unable to build LevelDB store");

		isopen = true;
		return true;
	}

	/**
	 * Write a block of data to the store, this block must be qualified by
	 * the DataBlock class, so that a data buffer and hash is provided.
	 * @param DataBlock to insert
	 * @return Whether the insertion of the block was successful.
	 */
	@Override
	public boolean insert(DataBlock blk)
		throws IOException
	{
		if(!isopen)
			throw new IOException("Database not open");
		if(lvldb == null)
			throw new IOException("Database not initialized");

		try {
			/*
			 * Simple as possible for now, toss the data in.
			 * This will throw */
			lvldb.put(blk.hash(), blk.data());

		} catch(DBException e) {
			throw new IOException("Failed to insert block: "
					      + e.toString());
		}

		return true;
	}
}
