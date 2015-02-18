/**
 * Copyright © 2014
 * Hrfs Hash Engine
 *
 * Instantiates a dynamic hash function engine that background processes block
 * for output hashes. This performs the given hash function using a supplied
 * hash function, for example SHA1, using the guava Hash library.
 *
 * @file HashEngine.java
 * @author Will Dignazio <wdignazio@gmail.com>
 */
package edu.rit.cs;

import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.io.IOException;
import com.google.common.hash.HashFunction;
import com.google.common.hash.HashCode;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class HashEngine
{
	static
	{
		HrfsConfiguration.init();
	}

	private static final Log LOG = LogFactory.getLog(HashEngine.class);
	private HrfsConfiguration conf;
	private ThreadPoolExecutor executor;
	private ConcurrentLinkedQueue<Block> biqueue;
	private ConcurrentLinkedQueue<DataBlock> boqueue;
	private HashFunction hashfn;
	private int nworkers;

	private class HEWorker
		implements Runnable
	{
		private Object _master;
		private HashFunction _phashfn;

		
		public HEWorker(Object master, HashFunction phashfn)
		{
			_master = master;
			_phashfn = phashfn;
		}

		@Override
		public void run()
			throws IllegalArgumentException
		{
			DataBlock dblk;
			HashCode hcode;
			byte[] bdata;
			Block blk;
			
			if(biqueue == null || boqueue == null)
				throw new IllegalArgumentException("Unitialized Engine");
			
			for(;;) {
				/*
				 * Wait up for the master engine instance to
				 * provide more block data. This should be a
				 * relatively inexpensive check compared to
				 * producing the contant block hashing.
				 */
				if(biqueue.isEmpty()) {
					try {
						synchronized(_master) {
							_master.wait();
						}
					} catch(InterruptedException e) {
						/* XXX */
					}
				}

				/*
				 * We may have /just/ missed the empty check, if
				 * that is the case, we will need to 'spin' until
				 * another block comes up for grabs.
				 */
				blk = biqueue.poll();
				if(blk == null)
					continue;

				bdata = blk.data();
				if(blk.data() == null)
					throw new IllegalArgumentException("Invalid Block/Data");

				/* Use the generic hash function to generate hash */
				hcode = _phashfn.newHasher()
					.putBytes(bdata)
					.hash();

				/* Go Go Go! */
				dblk = new DataBlock(blk.data(), hcode.asBytes(), blk.index());
				boqueue.add(dblk);
			}
		}
	}

	/**
	 * Construct a new hash engine, using the default configuration values
	 * present in the the hrfs site configuration file.
	 */
	public HashEngine()
	{
		this.conf = new HrfsConfiguration();
		this.nworkers = conf.getInt(HrfsKeys.HRFS_HENGINE_WORKERS, 5);

		this.biqueue = new ConcurrentLinkedQueue<Block>();
		this.boqueue = new ConcurrentLinkedQueue<DataBlock>();
		
		/* Build up our worker pool using the configuration tunable. */
		this.executor = new ThreadPoolExecutor(nworkers, nworkers,
						       1000L, TimeUnit.MILLISECONDS,
						       new LinkedBlockingQueue<Runnable>());
	}

	/**
	 * Get the number of workers assigned to this engine.
	 * @return Number of active workers.
	 */
	public int getWorkerCount()
	{ return this.nworkers; }
}

