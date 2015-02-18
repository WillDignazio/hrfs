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

import java.io.IOException;
import org.junit.Test;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Assert;

public class LevelDBStoreTest
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
		store = new LevelDBStore(path);
	}
}

