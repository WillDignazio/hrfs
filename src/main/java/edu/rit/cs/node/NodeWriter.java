/**
 * Copyright © 2014
 * Hadoop Replicating Filesystem Node Writer
 *
 * This is the node writer to the underlying filesystem storage
 * device. This writer currently assumes there is an underlying
 * filesystem that will support the creation and editing of files.
 */
package edu.rit.cs.node;

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

	private synchronized void _writeByteBuffer(byte[] buffer, int off, int len)
		throws IOException, AccessDeniedException
	{
		try {
			this.sha1 = getSHA1(buffer);
			file = new File(this.path + "/" + this.sha1);

			if(this.file.exists())
				throw new AccessDeniedException("Blocks cannot be modified.");

			file.createNewFile();
			fos = new FileOutputStream(file, false);

			/* Make sure we're not allowing reads yet */
			lock = fos.getChannel().lock();
			try {
				fos.write(buffer, off, len);
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

	/**
	 * Take a raw byte buffer and use it to a block of data to disk.
	 * This is identical to the char[] variant, but doesn't convert
	 * the data in the array.
	 * @param buf Byte buffer of the block.
	 * @param off Offset within the given buffer
	 * @param len Length to write to diskn
	 */
	public synchronized void write(byte[] buf, int off, int len)
		throws AccessDeniedException, IOException
	{
		this._writeByteBuffer(buf, off, len);
	}

	/**
	 * Implementation of write for the writer, takes a character
	 * array and writes it to disk.
	 * @param cbuf Character buffer to write to disk
	 * @param off Offset within buffer
	 * @param len Length to write to disk
	 */
	@Override
	public synchronized void write(char[] cbuf, int off, int len)
		throws AccessDeniedException, IOException
	{
		byte[] bytes;
		bytes = new String(cbuf).getBytes();
		this._writeByteBuffer(bytes, off, len);
	}

	/**
	 * Impelmentation of close for the writer, closes the
	 * stream to disk.
	 */
	@Override
	public void close()
		throws IOException
	{
		if(this.fos != null)
			fos.close();
	}

	/**
	 * Implementation of flush, makes sure that data has been
	 * written to disk.
	 */
	@Override
	public void flush()
		throws IOException
	{
		if(this.fos != null)
			fos.flush();
	}
}
