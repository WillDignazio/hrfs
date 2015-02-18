/**
 * Copyright © 2014
 * Hrfs LevelDB Block Store
 *
 * An implementation of a BlockStore that is backed using LevelDB.
 * By implementing the BlockStore API, higher layer can create, destroy, put,
 * remove, and check if blocks exist.
 *
 * @file LevelDBStore.java
 * @author Will Dignazio <wdignazio@gmail.com>
 */
package edu.rit.cs.disk;

import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.*;
import edu.rit.cs.HrfsKeys;
import edu.rit.cs.HrfsConfiguration;

class LevelDBStore
	implements BlockStore
{
	static
	{
		HrfsConfiguration.init();
	}

	private static final Log LOG = LogFactory.getLog(LevelDBStore.class);	

	private HrfsConfiguration conf;
	private String storePath;

	public LevelDBStore()
		throws IOException
	{
		String nodepath;

		storePath = conf.get(HrfsKeys.HRFS_NODE_STORE_PATH);
		if(storePath == null)
			throw new IOException("Store Path unset, refusing to construct store.");
	}

	@Override
	public boolean create()
	{
		return false;
	}
}
