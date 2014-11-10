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
import java.net.*;

import java.net.URISyntaxException;
import java.net.URI;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.*;
import org.apache.hadoop.fs.Options.ChecksumOpt;
import org.apache.hadoop.ipc.RPC;

import org.apache.zookeeper.*;
import org.apache.zookeeper.ZooDefs.*;

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
	private InetSocketAddress naddr;
	private ZooKeeper zk;
	private HrfsSession session;
	private HrfsRPC nrpc;

	/** Internal watch handler that listens for cluster changes. */
	private class ZooWatcher
		implements Watcher {
		@Override
		public void process(WatchedEvent event) {
			LOG.info("Connected to ZooKeeper");
		}
	}
	
	/**
	 * Default configuration, stub.
	 */
	public Hrfs()
	{
		try {
			this.conf = new HrfsConfiguration();
			this.naddr = new InetSocketAddress(
				conf.get(HrfsKeys.HRFS_NODE_ADDRESS, "127.0.0.1"),
				conf.getInt(HrfsKeys.HRFS_NODE_PORT, 60010));

			this.nrpc = RPC.getProxy(HrfsRPC.class,
					    RPC.getProtocolVersion(HrfsRPC.class),
					    naddr, conf);

			/* Setup the ZooKeeper session */
			this.zk = new ZooKeeper(
				conf.get(HrfsKeys.HRFS_ZOOKEEPER_ADDRESS, "127.0.0.1"),
				conf.getInt(HrfsKeys.HRFS_ZOOKEEPER_PORT, 2181),
				new ZooWatcher());
			
			/* Build client session */
			this.session = new HrfsSession(zk);
			
			LOG.info("Finished initializing hrfs connection.");
		}
		catch(IOException e) {
			LOG.error("Failed to instantiate hrfs connection: " +
				  e.toString());
			System.exit(1); // Just stop here
		}
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
	public Path getWorkingDirectory()
	{
		Path path;
		return new Path(this.session.getWorkingDirectory());
	}

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
