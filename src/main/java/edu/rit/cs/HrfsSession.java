/**
 * Copyright © 2014
 * Hadoop Replicating File System Session
 *
 * @file HrfsSession.java
 * @author Will Dignazio <wdignazio@gmail.com>
 */
package edu.rit.cs;

import java.io.*;
import java.util.*;
import java.net.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

import org.apache.zookeeper.*;
import org.apache.zookeeper.ZooDefs.*;

public class HrfsSession
	implements Serializable
{
	static
	{
		HrfsConfiguration.init();
	}

	private static final Log LOG = LogFactory.getLog(HrfsSession.class);
	private transient static final String SESSION_ZNODE_PATH = "/hrfs-session";
	private transient ZooKeeper zk;
	private transient String spath;
	
	private String workingDirectory;
	
	/**
	 * Establishes a new session object within the zookeeper service,
	 * and makes sure that object is initially nothing. This uses an 
	 * ephemeral-sequential node with a shared zookeeper path.
	 */
	public HrfsSession(ZooKeeper zk)
	{
		ByteArrayOutputStream bstream;
		ObjectOutputStream ostream;
		byte buffer[];

		this.zk = zk;
		this.workingDirectory = "/";
		
		/* XXX All objects need to be initialized before here */
		try {
			bstream = new ByteArrayOutputStream();
			ostream = new ObjectOutputStream(bstream);
			
			ostream.writeObject(this);
			buffer = bstream.toByteArray();
			this.spath = zk.create(SESSION_ZNODE_PATH,
					       buffer, // Session data
					       Ids.OPEN_ACL_UNSAFE,
					       CreateMode.EPHEMERAL_SEQUENTIAL);
		}
		catch(InterruptedException e) {
			LOG.error("Interrupted while building session: " +
				  e.toString());
			System.exit(1);
		}
		catch(KeeperException e) {
			LOG.error("Unable to form HrfsSession: " +
				  e.toString());
			System.exit(1); // Shouldn't go any farther
		}
		catch(IOException e) {
			LOG.error("Failed to parse HrfsSession: " +
				  e.toString());
			System.exit(1); // Here either
		}
	}

	/**
	 * Get the working directory for the session.
	 * @return directory Working directory of session
	 */
	public synchronized String getWorkingDirectory()
	{
		/* 
		 * XXX TODO: This later has the possibility of being
		 * updated from zookeeper, we need to setup a 
		 * watcher for it.
		 */
		return this.workingDirectory;
	}
	
	/**
	 * Changed the working directory for the Hrfs Session, this will
	 * also update the session data in the ZooKeeper server.
	 * @param directory New working directory
	 */
	public synchronized void setWorkingDirectory(String directory)
	{
		ByteArrayOutputStream bstream;
		ObjectOutputStream ostream;
		byte[] buffer;

		try {
			bstream = new ByteArrayOutputStream();
			ostream = new ObjectOutputStream(bstream);
			
			ostream.writeObject(this);
			buffer = bstream.toByteArray();

			/* Update the data in the session */
			zk.setData(spath, buffer, -1);
		}
		catch(InterruptedException e) {
			LOG.error("Interrupted while setting working directory: " +
				  e.toString());
		}
		catch(KeeperException e) {
			LOG.error("Keeper failure in zookeeper: " + e.toString());
		}
		catch(IOException e) {
			LOG.error("Failed to update session information: " +
				  e.toString());
		}
	}
}
