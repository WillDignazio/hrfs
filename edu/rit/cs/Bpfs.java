/**
 * Block Party File System
 *
 * @file Bpfs.java
 * @author Will Dignazio <wdignazio@gmail.com>
 */
package edu.rit.cs;

import java.io.*;
import java.net.URISyntaxException;
import java.net.URI;
import java.util.EnumSet;

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
public class Bpfs extends AbstractFileSystem
{
	/* Make sure to initialize conf only once */
	static
	{
		BpfsConfiguration.init();
	}

	public Bpfs(final URI uri, final Configuration conf)
		throws URISyntaxException
	{
		super(uri, BpfsConstants.BPFS_URI_SCHEME, 
		      true, BpfsConstants.BPFS_PORT);
	}

	@Override
	public void checkPath(Path path) { };

	@Override
	public void checkScheme(URI uri, String scheme) { };

	@Override
	public void setVerifyChecksum(boolean verifyChecksum) { };

	@Override
	public FileStatus[] listStatus(Path path) { return null; };

	@Override
	public FsStatus getFsStatus(Path path) { return null; };

	@Override
	public FsStatus getFsStatus() { return null; };

	@Override
	public BlockLocation[] getFileBlockLocations(Path path, long start, long len) { return null; }

	@Override
	public FileStatus getFileStatus(Path path) { return null; }

	@Override
	public FileChecksum getFileChecksum(Path path) { return null; }

	@Override
	public void setTimes(Path path, long mtime, long atime) { };

	@Override
	public void setOwner(Path path, String username, String groupname) { };

	@Override
	public void setPermission(Path path, FsPermission perms) { };

	@Override
	public void renameInternal(Path src, Path dst) { };

	@Override
	public boolean setReplication(Path path, short replication) { return false; };

	@Override
	public FSDataInputStream open(Path path, int buffersize) { return null; }

	@Override
	public boolean delete(Path path, boolean recursive) { return false; }

	@Override
	public void mkdir(Path path, FsPermission perms, boolean createParent) { }

	@Override
	public FSDataOutputStream createInternal(Path path, EnumSet<CreateFlag> flag,
						 FsPermission absPerms, int buffersz,
						 short replication, long blksz,
						 Progressable progress, ChecksumOpt chksumopt,
						 boolean createParent) { return null; }

	@Override
	public FsServerDefaults getServerDefaults() { return null; }

	@Override
	public int getUriDefaultPort() { return -1; }
}
