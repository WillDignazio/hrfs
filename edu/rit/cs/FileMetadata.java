/**
 * File Metadata Wrapper Class
 *
 */
package edu.rit.cs;

import org.apache.hadoop.hdfs.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

import java.io.*;
import java.util.*;
import java.net.*;

public class FileMetadata
{
	private FileSystem fs;
	private FileStatus fstatus;
	private Path path;

	/**
	 * Builds the file metadata, polls from the filesystem
	 * the particular files replication factor.
	 */
	public FileMetadata(DistributedFileSystem fs, Path path)
	{
		try {
			this.path = path;
			this.fs = fs;
			fstatus = fs.getFileStatus(path);
		}
		catch(IOException io) {
			System.err.println("Failed to get file status (metadata)");
			return;
		}
	}

	/**
	 * Gets the number of blocks that compose the file, this is varying
	 * depending on the filesystem configuration.
	 */
	public synchronized long blockCount()
	{
		long blksz;
		long len;
		long nblk;

		blksz = fstatus.getBlockSize();
		len = fstatus.getLen();

		nblk = blksz / len;
		if(nblk == 0)
			nblk = 1;

		return nblk;
	}

	/**
	 * Simple wrapper that gets the replica factor of the file,
	 * used for quick recall of the replication value.
	 */
	public synchronized short replicaCount()
	{
		return fstatus.getReplication();
	}

	/**
	 * Wrapper that increments the number of replicas for the
	 * file. Goes therough the FileStatus.
	 */
	public synchronized boolean incReplication()
	{
		short replicas;
		boolean set;

		try {
			replicas = this.replicaCount();
			set = fs.setReplication(path, ++replicas);
		}
		catch(IOException e) {
			System.err.println("Failed to increment file replication.");
			set = false;
		}

		return set;
	}
}
