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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.LinkedBlockingQueue;
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

	private ThreadPoolExecutor executor;
	private HrfsConfiguration conf;
	private String storePath;
	private Options options;
	private AtomicBoolean isopen;
	private File lvlfd;
	private DB lvldb;

	/**
	 * Helper class that can be used to asynchronously store a block of data
	 * given to the underlying block storage. This will not acknowledge that
	 * acknowledge completeness currently, but give an exception for failed
	 * writes.
	 */
	private class LevelDBWorker
		extends Thread
	{
		DataBlock _blk;
		
		public LevelDBWorker(DataBlock blk)
		{
			_blk = blk;
		}

		/* Simpley go and insert the block */
		@Override
		public void run()
		{
			if(lvldb == null || !isopen.get()) {
				System.err.println("Invalid LevelDB instance for worker");
				return;
			}

			try {
				/*
				 * Simple as possible for now, toss the data in.
				 * This will throw */
				lvldb.put(_blk.hash(), _blk.data());
				
			}
			catch(DBException e) {
				System.err.println("Failed to insert block: "
						      + e.toString());
				return;
			}
		}
	}
	
	/**
	 * Construct a LevelDB Block Store instance, this will not create or open
	 * store outright, but instead give a handle to a store as configured in
	 * the site conf.
	 * Use the BlockStore API to open, create, or use a block store.
	 */
	public LevelDBStore(String path, int nworkers)
		throws IOException
	{
		String nodepath;

		storePath = path;
		if(storePath == null)
			storePath = conf.get(HrfsKeys.HRFS_NODE_STORE_PATH);

		if(storePath == null)
			throw new IOException("Store Path unset, refusing to construct store.");		

		isopen = new AtomicBoolean(false);
		
		options = new Options();
		options.compressionType(CompressionType.NONE);

		lvlfd = new File(storePath);
		if(lvlfd.exists() && (!lvlfd.canWrite() || !lvlfd.canRead()))
			throw new IOException("Insufficient Permission");

		/* Build up our worker pool using the configuration tunable. */
		this.executor = new ThreadPoolExecutor(nworkers, nworkers,
						       1000L, TimeUnit.MILLISECONDS,
						       new LinkedBlockingQueue<Runnable>());
	}

	/**
	 * Return whether the LevelDB store is open.
	 * @return Whether the store is open.
	 */
	@Override
	public boolean isOpen()
	{ return isopen.get(); }		
	
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

		isopen.set(true);
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

		isopen.set(true);
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
		if(!isopen.get())
			throw new IOException("Database not open");
		if(lvldb == null)
			throw new IOException("Database not initialized");

		executor.execute(new LevelDBWorker(blk));
		return true;
	}
}
