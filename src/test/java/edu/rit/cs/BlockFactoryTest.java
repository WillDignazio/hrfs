/**
 * Copyright @ 2014
 * Hrfs Raw Block Factory Tests
 *
 * @file BlockFactoryTest.java
 * @author Will Dignazio <wdignazoi@gmail.com>
 */
package edu.rit.cs;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.io.FileUtils;
import java.io.IOException;
import java.io.File;
import java.io.ByteArrayOutputStream;
import java.util.HashMap;
import org.junit.Test;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Assert;

import edu.rit.cs.HrfsConfiguration;

public class BlockFactoryTest
{
	private Environment tenv;


	@Before
	public void initTest()
	{
		HrfsConfiguration.init();

		try {
			/* Build up our test environment, try to share it */
			tenv = new Environment(TestUtil.TEST_BASE + "rawblocks/");
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
	public void testFileConsistency()
		throws IOException
	{
		ByteArrayOutputStream bos;
		BlockFactory factory;
		int blksz;
		Block blk;
		File file;
		int blks;
		byte[] bfile;
		byte[] bfactory;
		
		/* 64KB file Blocks */
		blksz = 1024*64;
		file = tenv.createFile(blksz * 100);

		bos = new ByteArrayOutputStream();
		bfile = FileUtils.readFileToByteArray(file);
		factory = new BlockFactory(file, blksz);

		blks=0;
		while(!factory.isDone())
		{
			blk = factory.getBlock();

			if(blk != null) {
				++blks;
				/* This is OK since we're specifying a < MAX_INT blksz */
				bos.write(blk.data(), 0, (int)blk.length());
			}
		}

		bos.flush();
		bfactory = bos.toByteArray();

		/* Compare to length, since last block may be padded */
		for(int bidx=0; bidx < bfile.length; ++bidx)
			Assert.assertEquals(bfile[bidx], bfactory[bidx]);
	}

	/**
	 * Test a file that does not have a count of blocks that matches the
	 * block size given to the factory. The factory should produce an
	 * number of blocks that is the number of full blocks within the file,
	 * plus an additional one for the "short" block.
	 */
	@Test
	public void testUnalignedFileCount()
		throws IOException
	{
		BlockFactory factory;
		HashMap<Long, Block> tmap;
		int blksz;
		Block blk;
		File file;
		int blks;

		tmap = new HashMap<Long, Block>();

		/* 64 MB blksz */
		blksz = 1024*1024*64;

		/* 4 Blocks, plus 500 bytes of random data */
		file = tenv.createFile((blksz * 4) + 500);
		
		/* 64 MB Block Size */
		factory = new BlockFactory(file, blksz);

		blks=0;
		while(!factory.isDone())
		{
			blk = factory.getBlock();

			if(blk != null) {
				++blks;

				Assert.assertEquals(blksz, blk.length());
				Assert.assertFalse(tmap.containsKey(blk.index()));
				tmap.put(blk.index(), blk);
			}
		}
		
		Assert.assertEquals(5, blks);
	}
	
	@Test
	public void testByteArrayBlockCount()
		throws IOException
	{
		BlockFactory factory;
		HashMap<Long, Block> tmap;
		int blksz;
		Block blk;
		byte[] barr;
		int blks;

		tmap = new HashMap<Long, Block>();
		/* 64kb blksz */
		blksz = 1024*64;

		barr = new byte[blksz * 1024];
		factory = new BlockFactory(barr, blksz);

		blks = 0;
		while(!factory.isDone())
		{
			blk = factory.getBlock();
			if(blk != null) {
				++blks;
				Assert.assertEquals(blksz, blk.length());
				Assert.assertFalse(tmap.containsKey(blk.index()));
				tmap.put(blk.index(), blk);
			}
		}

		Assert.assertEquals(1024, blks);
	}
	
	@Test
	public void testFileBlockCount()
		throws IOException
	{
		BlockFactory factory;
		HashMap<Long, Block> tmap;
		int blksz;
		Block blk;
		File file;
		int blks;

		tmap = new HashMap<Long, Block>();
		
		blksz = 1024*1024*64;
		file = tenv.createFile(blksz * 5);
		
		/* 64 MB Block Size */
		factory = new BlockFactory(file, blksz);

		blks=0;
		while(!factory.isDone())
		{
			blk = factory.getBlock();

			if(blk != null) {
				++blks;
				Assert.assertEquals(blksz, blk.length());
				Assert.assertFalse(tmap.containsKey(blk.index()));			
				tmap.put(blk.index(), blk);
			}
		}
		
		Assert.assertEquals(5, blks);
	}
}
