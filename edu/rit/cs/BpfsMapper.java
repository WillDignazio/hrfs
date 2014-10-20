/**
 * Copyright © 2014
 * Block Party Filesystem Mapper
 *
 * Mapper: Path+File => [(Blk#, Blk)...]
 */
package edu.rit.cs;

import java.io.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class BpfsMapper
	extends	MapReduceBase		
	implements Mapper<Text, File, LongWritable, BytesWritable>
{
	static
	{
		BpfsConfiguration.init();
	}

	BpfsConfiguration conf = new BpfsConfiguration();
	LongWritable wblki = new LongWritable();	// Writable block index
	BytesWritable wblka = new BytesWritable();	// Writable block data

	private int blocksz = conf.getInt(BpfsKeys.BPFS_BLKSZ, 65536);

	@Override
	public void map(Text path, File file, 
			OutputCollector<LongWritable, BytesWritable> output,
			Reporter reporter) throws IOException
	{
		InputStream fin;
		byte[] blkarr;
		long nblocks;

		/* Must have at least one block of data to be written */
		nblocks = file.length() / blocksz;
		if(nblocks == 0)
			++nblocks;
		
		blkarr = new byte[this.blocksz];
		fin = new FileInputStream(file);

		for(long blk=0; blk < nblocks; ++blk)
		{
			int nread;
			
			nread = fin.read(blkarr);
			//XXX What if not block aligned?

			/* Establish map values, collect them together */
			wblki.set(blk);
			wblka.set(blkarr, 0, blocksz);
			output.collect(wblki, wblka);
		}
	}
}
