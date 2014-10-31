/**
 * Copyright © 2014
 * Hadoop Replicating File System
 *
 * @file Hrfs.java
 * @author Will Dignazio <wdignazio@gmail.com>
 */
package edu.rit.cs;

import java.io.*;
import java.util.*;

import java.net.URISyntaxException;
import java.net.URI;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.*;
import org.apache.hadoop.fs.Options.ChecksumOpt;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class Hrfs extends FileSystem
{	
	/* Make sure to initialize conf only once */
	static
	{
		HrfsConfiguration.init();
	}

	private static final Log LOG = LogFactory.getLog(Hrfs.class);

	private HrfsConfiguration conf;
	private String[] nodeHosts;
	private List<HrfsRPC> rpcs;

	/**
	 * Default configuration, stub.
	 */
	public Hrfs()
	{
		conf = new HrfsConfiguration();
		nodeHosts = conf.getStrings(HrfsKeys.HRFS_CLIENT_NODES);
		rpcs = new ArrayList<HrfsRPC>();
	}

	/**
	 * Get configuration scheme, generally hrfs://
	 */
	@Override
	public String getScheme()
	{ 
		return HrfsKeys.HRFS_DEFAULT_URI_SCHEME;
	}

	@Override
	public void initialize(URI uri, Configuration conf)
		throws IOException
	{
		super.initialize(uri, conf);
		setConf(conf);
	}

	@Override
	public FileStatus getFileStatus(Path p) { return null; }

	@Override
	public boolean mkdirs(Path p, FsPermission permission) { return false; };

	@Override
	public Path getWorkingDirectory() { return null; }

	@Override
	public void setWorkingDirectory(Path p) { }

	@Override
	public FileStatus[] listStatus(Path p) { return null; }

	@Override
	public boolean delete(Path p, boolean recursive) { return false; }

	@Override
	public boolean rename(Path src, Path dst) { return false; }

	@Override
	public FSDataOutputStream append(Path p, int buffersize) { return null; }

	@Override
	public FSDataOutputStream append(Path p, int buffersize, Progressable progress)
		throws IOException { return null; }

	public FSDataOutputStream create(Path f)
	{
		MetadataBlock mblk;

		/* Create empty block */
		mblk = new MetadataBlock(f.toString());

		return null;
	}

	@Override
	public FSDataOutputStream create(Path f, FsPermission permission, boolean overwrite, 
					 int bufferSize, short replication, 
					 long blockSize, Progressable progress) 
	{
		return null;
	}

	@Override
	public FSDataInputStream open(Path p, int buffersize) { return null; }

	@Override
	public URI getUri() { return null; }
}
