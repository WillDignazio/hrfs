/**
 * BlockParty Reader
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

import edu.rit.cs.BlockPartyClient;

public class BlockPartyReader
	extends BufferedReader
{
	BlockPartyClient client;

	public BlockPartyReader(BlockPartyClient client, InputStreamReader ireader)
	{
		super(ireader);
		this.client = client;
	}

	/**
	 * Wrapper method that performs the block replication
	 * count. This simply calls the parent reader object's
	 * readLine, but also appropriately modifies the blocks
	 * 'popularlity'.
	 */
	@Override
	public String readLine()
		throws IOException
	{
		String base;

		/* Grab the parent's string */
		base = super.readLine();

		return base;
	}
}
