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
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import com.google.common.hash.HashFunction;
import com.google.common.hash.HashCode;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import java.io.IOException;

public class HashEngine
{
	static
	{
		HrfsConfiguration.init();
	}

	private static final Log LOG = LogFactory.getLog(HashEngine.class);
	private AtomicLong ahcnt;
	private HrfsConfiguration conf;
	private ThreadPoolExecutor executor;
	private ConcurrentLinkedQueue<Block> biqueue;
	private ConcurrentLinkedQueue<DataBlock> boqueue;
	private HashFunction hashfn;
	private int nworkers;

	private class HEWorker
		implements Runnable
	{
		private HashFunction _phashfn;
		private Block _blk;
		
		public HEWorker(HashFunction phashfn, Block blk)
		{
			_phashfn = phashfn;
			_blk = blk;
		}

		@Override
		public void run()
			throws IllegalArgumentException
		{
			DataBlock dblk;
			HashCode hcode;
			byte[] bdata;
			
			if(biqueue == null || boqueue == null || ahcnt == null)
				throw new IllegalArgumentException("Unitialized Engine");
			
			for(;;) {
				bdata = _blk.data();
				if(_blk.data() == null)
					throw new IllegalArgumentException("Invalid Block/Data");

				/* Use the generic hash function to generate hash */
				hcode = _phashfn.newHasher()
					.putBytes(bdata)
					.hash();

				/* Go Go Go! */
				dblk = new DataBlock(_blk.data(), hcode.asBytes(), _blk.index());
				boqueue.add(dblk);
				/* For now we need a hash metric */
			}
		}
	}

	/**
	 * Construct a new hash engine, using the default configuration values
	 * present in the the hrfs site configuration file.
	 */
	public HashEngine(HashFunction hfn)
	{
		this.conf = new HrfsConfiguration();
		this.hashfn = hfn;
		this.nworkers = conf.getInt(HrfsKeys.HRFS_HENGINE_WORKERS, 5);

		this.biqueue = new ConcurrentLinkedQueue<Block>();
		this.boqueue = new ConcurrentLinkedQueue<DataBlock>();
		this.ahcnt = new AtomicLong(0);
		
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

	/**
	 * Get the number of DataBlocks produced by this HashEngine.
	 * This value is atomic and is the true representation of the number of
	 * blocks processed bye the hash engine.
	 */
	public long getProducedCount()
	{ return this.ahcnt.get(); }

	/**
	 * Submit a job to run within the HashEngine, this passes the work off
	 * a waiting thread. If no such thread is present, the thread that was
	 * asked to the job will yield, and the thread that requested the
	 * insertion will complete the task.
	 */
	public void putBlock(Block blk)
	{
		if(blk == null || blk.data() == null)
			throw new IllegalArgumentException("Invalid block");

		/* 
		 * Safely and concurrently add the block to the biqueue object
		 * within the engine. The biqueue uses a thread safe data structure
		 * to back it's primary storae.
		 */
		executor.execute(new HEWorker(hashfn, blk));
	}

	/**
	 * Poll for a data block from the engine, if no new data blocks are
	 * available, then null is returned. 
	 */
	public DataBlock getDataBlock()
	{
		DataBlock dblk;

		/** XXX Needs to be fixed */
		dblk = boqueue.poll();
		return dblk;
	}
}
