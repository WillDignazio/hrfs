/**
 * Copyright @ 2014
 * Hrfs Hash Engine Tests
 *
 * @file HashEngineTests.java
 * @author <wdignazio@gmail.com>
 */
package edu.rit.cs;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import java.util.Random;
import java.io.IOException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class HashEngineTest
{
	private Environment tenv;

	@Before
	public void initTest()
	{
		HrfsConfiguration.init();

		try {
			/* Build up our test environment, try to share it */
			tenv = new Environment(TestUtil.TEST_BASE + "hashengine/");
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
	public void createSHA1HEngineTest()
	{
		HashEngine hengine;
		HashFunction hfn;

		hfn = Hashing.sha1();
		hengine = new HashEngine(hfn);

		Assert.assertFalse(hengine == null);
		Assert.assertTrue(hengine.getWorkerCount() > 0);
		Assert.assertTrue(hengine.getProducedCount() == 0);
	}

	@Test
	public void hashSHA1HEngineTest()
		throws IOException
	{
		Random rnd;
		BlockFactory factory;
		HashEngine hengine;
		HashFunction hfn;
		byte[] tbuf;
		int cnt;

		cnt = 0;
		rnd = new Random();
		tbuf = new byte[1024*1024*10]; // 10MB
		rnd.nextBytes(tbuf);

		hfn = Hashing.sha1();
		hengine = new HashEngine(hfn);

		/* Create 64KB blk factory on in-memory data */
		factory = new BlockFactory(tbuf, 1024*64);

		while(!factory.isDone()) {
			Block blk;

			blk = factory.getBlock();
			if(blk == null)
				break;

			hengine.hashBlock(blk);
		}
	}
}
