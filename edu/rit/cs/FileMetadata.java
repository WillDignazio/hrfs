/**
 * File Metadata Wrapper Class
 *
 */
package edu.rit.cs;

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
	public FileMetadata(FileSystem fs, Path path)
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
}
