/**
 * Copyright © 2014
 * Hadoop Replicating Filesystem Node Reader
 * 
 * Node reader to the underlying filesystem storage
 * device. Will read data from the given path, as according
 * the data written by the NodeWriter.
 *
 */
import edu.rit.cs.node;

import java.io.Reader;

public class NodeReader
	extends Reader
{
	private String path;

	public NodeReader(String basedir, String sha1)
		throws FileNotFoundException
	{
		super();
		File fpath;

		this.path = basedir;
		fpath = new File(path);

		if(!fpath.exists() || !fpath.isDirectory())
			throw new FileNotFoundException("Base directory does not exist");
	}

	
}
