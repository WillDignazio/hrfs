/**
 * Copyright @ 2014
 * Hrfs Hash Engine Tests
 *
 * @file HashEngineTests.java
 * @author <wdignazio@gmail.com>
 */
package edu.rit.cs;

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
}
