/**
 * Copyright © 2015
 * Hadoop Replicating File System
 *
 * Test class for the on disk data structures.
 *
 * @file DiskTest.java
 * @author Will Dignazio <wdignazio@gmail.com>
 */
package edu.rit.cs.disk;

import java.io.RandomAccessFile;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.File;
import java.nio.file.Paths;
import java.util.Random;
import java.util.UUID;

import junit.framework.Assert;
import org.junit.Test;

import edu.rit.cs.disk.*;

public class DiskTest
{
	/**
	 * Generate a zero-filled test file.
	 * @param size Size of test file
	 * @param perm Permisssions of test file
	 */
	public String createZeroedTestFile(long size, String perm)
		throws IOException
	{
		String filename;
		RandomAccessFile rf;

		filename = "test-" + UUID.randomUUID().toString();
		rf = new RandomAccessFile(filename, perm);
		rf.setLength(size);

		return filename;
	}

	/**
	 * Generate a randomly-filled test file.
	 * @param size Size of test file
	 * @param perm Permissions of test file
	 */
	public String createRandomTestFile(long size, String perm)
		throws IOException
	{
		Random rand;
		String filename;
		RandomAccessFile rf;

		filename = "test-" + UUID.randomUUID().toString();
		rand = new Random();
		rf = new RandomAccessFile(filename, perm);

		for(long p=0; p < size; ++p)
			rf.write((byte)rand.nextInt());

		return filename;
	}

	@Test
	public void testDiskCreation()
		throws IOException
	{
		HrfsDisk hdisk;
		String fname;

		// 10mb test file
		fname = createRandomTestFile(1024*1024*10, "rw");
		hdisk = new HrfsDisk(Paths.get(fname));

		new File(fname).delete();
	}
}
