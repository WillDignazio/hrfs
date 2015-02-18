/**
 * Copyright @ 2014
 * Hrfs Test Environment
 *
 * @file Environment.java
 * @author Will Dignazio <wdignazio@gmail.com>
 */
package edu.rit.cs;

import java.util.UUID;
import java.util.Random;
import java.security.PermissionCollection;
import org.apache.commons.io.FileUtils;
import java.io.IOException;
import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.FilePermission;
import java.io.File;

public class Environment
{
	public static final long QUOTA_MAX = 1024L*1024L*1024L*2L; // 2GB max
	private File basedir;
	private FilePermission defperms;
	private PermissionCollection envperms;
	private long uquote;
	private long quota;

	/** Need at least a base directory */
	private Environment() { }

	/**
	 * Open up a new test environment based on the specified path. This
	 * will test the given path for write and read protection, and
	 * produced file descriptors will remain within this environment.
	 * @param path Path to base of test environment
	 */
	public Environment(String path)
		throws IOException, SecurityException
	{
		basedir = new File(path);
		if(!basedir.isDirectory()) {
			if(basedir.isFile())
				throw new IOException("Base must be a directory.");

			/* We don't want to assume it already exists */
			if(!basedir.exists())
				basedir.mkdirs();
		}

		if(basedir.list().length > 0) {
			if(path.equals("/"))
				throw new IOException("Cowardly refusing to delete everything");
			else if(path.equals("."))
				throw new IOException("Cowardly refusing to delete your work");
			else if(path.equals(".."))
				throw new IOException("C'mon man, you're going to lose your data.");
			else if(path.equals(System.getenv("user.home")))
				throw new IOException("Now you're just messing with me");

			/* Clean out the files from previous tests */
			FileUtils.cleanDirectory(basedir);
		}

		/* Set default permissions for all files under base */
		defperms = new FilePermission(path + "/-", "read,write");
		envperms = defperms.newPermissionCollection();
		envperms.add(defperms);

		uquote = 0;
		quota = QUOTA_MAX;
	}

	/** Build up the environment with the desired quota */
	public Environment setQuota(long quota)
	{
		this.quota = quota;
		return this;
	}

	/** Return the quota for the environment */
	public long getQuota()
	{ return this.quota; }

	/** Get a quote of the users quota consumption */
	public long getQuotaUsage()
	{ return this.uquote; }
	
	/**
	 * Returns the base string of the test environment.
	 * @return base Base path to environment.
	 */
	public String getBasePath()
	{
		if(basedir == null)
			return "";

		return basedir.getAbsolutePath();
	}

	/**
	 * Retrieve a new file descriptor from the base directory of the
	 * environment. The file will by default have read and write permissions.
	 * @return file File descriptor within environment.
	 */
	public File createFile()
		throws SecurityException, IOException
	{
		File file;
		String path;

		path = getBasePath() + "/testfile-" + UUID.randomUUID();
		file = new File(path);
		envperms.add(new FilePermission(path, "read,write"));
				    
		return file;
	}

	/**
	 * Generate a randomly filled file of the given size. The size
	 * of this file will be deducted from the quota given to the
	 * environment.
	 * @param size Size of randomly filled file.
	 */
	public synchronized File createFile(long size)
		throws SecurityException, IOException
	{
		BufferedOutputStream writer;
		byte[] rndbuf;		
		Random rand;
		File file;
		
		/* Create base file */
		file = createFile();
		writer = new BufferedOutputStream(new FileOutputStream(file));

		if(size > getQuota() || uquote + size > getQuota())
			throw new IOException("Insufficient Space");

		uquote += size;
		rndbuf = new byte[4096];
		rand = new Random();

		long bidx;
		for(bidx=0; bidx < (size-bidx); bidx+=rndbuf.length) {
			rand.nextBytes(rndbuf);
			writer.write(rndbuf);
		}

		for(int rdx=0; rdx < size-bidx; ++rdx)
			writer.write((byte)rand.nextInt());

		writer.flush();
		return file;
	}
}
