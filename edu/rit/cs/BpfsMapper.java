/**
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
	LongWritable wblki = new LongWritable();	// Writable block index
	BytesWritable wblka = new BytesWritable();	// Writable block data

	@Override
	public void map(Text path, File file, 
			OutputCollector<LongWritable, BytesWritable> output,
			Reporter reporter) throws IOException
	{
		InputStream fin;
		byte[] blkarr;
		long nblocks;

		/* Must have at least one block of data to be written */
		nblocks = file.length() / BpfsConstants.BPFS_BLKSZ;
		if(nblocks == 0)
			++nblocks;
		
		blkarr = new byte[BpfsConstants.BPFS_BLKSZ];
		fin = new FileInputStream(file);

		for(long blk=0; blk < nblocks; ++blk)
		{
			int nread;
			
			nread = fin.read(blkarr);
			//XXX What if not block aligned?

			/* Establish map values, collect them together */
			wblki.set(blk);
			wblka.set(blkarr, 0, BpfsConstants.BPFS_BLKSZ);
			output.collect(wblki, wblka);
		}
	}
}
