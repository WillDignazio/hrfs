/**
 * Copyright © 2014
 * Hadoop Replicating Filesystem Node Writer
 */
package edu.rit.cs;

import java.io.IOException;
import java.io.FileNotFoundException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.Writer;
import java.nio.file.AccessDeniedException;
import java.nio.channels.FileLock;
import java.nio.CharBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;

public class NodeWriter
	extends Writer
{
	private FileLock lock;
	private FileOutputStream fos;
	private File file;
	private String path;
	private String sha1;
	private boolean placed;

	/**
	 * Construct a block writer based on a base directory
	 * for which it will be stored.
	 */
	public NodeWriter(String basedir)
		throws FileNotFoundException
	{

		super();
		File fpath;

		this.path = basedir;
		fpath = new File(path);

		this.file = null;
		this.fos = null;
		this.lock = null;

		/* Check that the data dir exists */
		if(!fpath.exists() || !fpath.isDirectory())
			throw new FileNotFoundException("Base directory does not exist");
	}

	/** Has the block been set? */
	public boolean isPlaced()
	{
		return this.placed;
	}

	/** What was the filename (sha1sum) */
	public String blockName()
	{
		return this.sha1;
	}

	/** Compute SHA1 Sum of block */
	private String getSHA1(byte[] buf)
		throws NoSuchAlgorithmException
	{
		Formatter formatter;
		MessageDigest md;

		md = MessageDigest.getInstance("SHA-1");
		formatter = new Formatter();

		for(byte b : md.digest(buf))
			formatter.format("%02x", b);
		
		return formatter.toString();
	}

	@Override
	public synchronized void write(char[] cbuf, int off, int len)
		throws AccessDeniedException, IOException
	{
		byte[] bytes;

		try {
			bytes = new String(cbuf).getBytes();
			this.sha1 = getSHA1(bytes);
			
			/* XXX This won't hold up to time */
			file = new File(this.path + "/" + this.sha1);

			/* Our blocks are immutable */
			if(this.file.exists())
				throw new AccessDeniedException("Blocks cannot be modified.");

			/* XXX Possible race condition */
			file.createNewFile();
			fos = new FileOutputStream(file, false);

			/* Make sure we're not allowing reads yet */
			lock = fos.getChannel().lock();
			try {
				fos.write(bytes, off, len);
				fos.flush();
				this.placed = true;
			}
			finally {
				lock.release();
			}
		}
		catch(NoSuchAlgorithmException e) { 
			System.err.println("SHA-1 Not Supported on System.");
			return;
		}
	}

	@Override
	public void close()
		throws IOException
	{
		if(this.fos != null)
			fos.close();
	}

	@Override
	public void flush()
		throws IOException
	{
		if(this.fos != null)
			fos.flush();
	}
}