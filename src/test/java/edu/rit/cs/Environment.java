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
	{
		basedir = new File(path);

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
