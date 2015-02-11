/**
 * Copyright @ 2014
 * Hrfs Raw Block Factory Tests
 *
 * @file RawBlockFactoryTest.java
 * @author Will Dignazio <wdignazoi@gmail.com>
 */
package edu.rit.cs;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import java.io.IOException;
import org.junit.Test;

import edu.rit.cs.HrfsConfiguration;

public class RawBlockFactoryTest
{
	private static final Log LOG = LogFactory.getLog(RawBlockFactoryTest.class);
	private static Environment tenv;

	static
	{
		HrfsConfiguration.init();

		try {
			/* Build up our test environment, try to share it */
			tenv = new Environment(TestUtil.TEST_BASE + "rawblocks");
		}
		catch(IOException e) {
			LOG.error("Error building test environment: " + e.toString());
			System.exit(1);
		}
		catch(SecurityException e){
			LOG.error("Insufficient/Invalid permissions for test environment: "
				  + e.toString());
			System.exit(1);
		}
	}

	@Test
	public void blockCountTest()
	{
	}
}


