/**
 * Copyright @ 2014
 * Hrfs Test Environment
 *
 * @file Environment.java
 * @author Will Dignazio <wdignazio@gmail.com>
 */
package edu.rit.cs;

import java.util.UUID;
import java.security.PermissionCollection;
import org.apache.commons.io.FileUtils;
import java.io.IOException;
import java.io.FilePermission;
import java.io.File;

class Environment
{
	private File basedir;
	private FilePermission defperms;
	private PermissionCollection envperms;

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

		if(!envperms.implies(new FilePermission(path, "read,write")))
			throw new SecurityException("Insufficient Environment Permissions");
	}

	/**
	 * Returns the base string of the test environment.
	 * @return base Base path to environment.
	 */
	public String getBasePath()
	{
		if(basedir == null)
			return null;

		return basedir.getAbsolutePath();
	}
	
	/**
	 * Retrieve a new file descriptor from the base directory of the
	 * environment. The file will by default have read and write permissions.
	 * @return file File descriptor within environment.
	 */
	public File getFile()
		throws SecurityException, IOException
	{
		File file;
		String path;

		path = getBasePath() + "testfile-" + UUID.randomUUID();
		file = new File(path);
		envperms.add(new FilePermission(path, "read/write"));
				    
		return file;
	}
}
