/**
 * Copyright @ 2015
 * Hrfs LevelDB Store Tests
 *
 * @file LevelDBStoreTest.java
 * @author Will Dignazio <wdignazio@gmail.com>
 */
package edu.rit.cs.disk;

import edu.rit.cs.DataBlock;
import edu.rit.cs.Environment;
import edu.rit.cs.TestUtil;
import edu.rit.cs.HashEngine;
import edu.rit.cs.BlockFactory;
import edu.rit.cs.Block;

import java.io.IOException;
import java.io.File;
import com.google.common.hash.Hashing;
import com.google.common.hash.HashFunction;
import org.junit.Test;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Assert;
import org.junit.Ignore;
import com.carrotsearch.junitbenchmarks.AbstractBenchmark;

public class LevelDBStoreTest
	extends AbstractBenchmark
{
	private Environment tenv;

	@Before
	public void initTest()
	{
		try {
			/* Build up our test environment, try to share it */
			tenv = new Environment(TestUtil.TEST_BASE + "leveldb/");
			Assert.assertNotNull(tenv);
		}
		catch(IOException e) {
			System.err.println("Error building test environment: " + e.toString());
		}
		catch(SecurityException e){
			System.err.println("Insufficient/Invalid permissions for test environment: "
				  + e.toString());
		}
	}

	@Test
	public void createTest()
		throws IOException
	{
		LevelDBStore store;
		String path;

		path = tenv.createFile().getAbsolutePath();
		store = new LevelDBStore(path, 2);

		Assert.assertFalse(store.isOpen());
		Assert.assertTrue(store.create());
		Assert.assertTrue(store.isOpen());
	}

	@Test
	public void insertTest()
		throws IOException
	{
		HashFunction hfn;
		HashEngine hengine;
		BlockFactory factory;
		File datafd;
		String path;
		int blksz;

		hfn = Hashing.sha1();
		blksz = 1024*64;
		path = tenv.createFile().getAbsolutePath();
		datafd = tenv.createFile(blksz * 50); // 50 Raw Blocks

		factory = new BlockFactory(datafd, blksz);
		hengine = new HashEngine(hfn);

		int blks = 0;
		while(!factory.isDone())
		{
			Block pblock;

			pblock = factory.getBlock();
			if(pblock != null) {
				hengine.putBlock(pblock);
				++blks;
			}
			else {
				continue;
			}
		}

		for(int blk=0; blk<blks; ++blk) {
			DataBlock dblock;

			System.out.println("Storing block: " + blk);
			dblock = hengine.getDataBlock();
			if(dblock == null) {
				blk--;
				continue;
			}
		}
	}
}
