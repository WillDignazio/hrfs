/**
 * Copyright © 2014
 * Hadoop Replicating Filesystem Writer
 *
 * @file HrfsWriter.java
 * @author Will Dignazio <wdignazio@gmail.com>
 */
package edu.rit.cs;

import java.io.*;
import java.net.*;
import java.util.*;
import org.apache.hadoop.ipc.RPC;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import edu.rit.cs.HrfsConfiguration;

public class HrfsWriter
{
	static
	{
		HrfsConfiguration.init();
	}

	private static final Log LOG = LogFactory.getLog(HrfsWriter.class);
	private FileInputStream fin;
	private HrfsConfiguration conf;
	private HrfsRPC rpc;
	
	public HrfsWriter(String path, HrfsRPC serv)
	{
		try {
			conf = new HrfsConfiguration();
			fin = new FileInputStream(path);
			this.rpc = serv;
		}
		catch(FileNotFoundException e) {
			LOG.error("File not found: " + path);
		}
	}

	public void publish()
	{
		byte buffer[];
		int c;

		c = -1;
		buffer = new byte[conf.getInt(HrfsKeys.HRFS_BLKSZ, 65536)];

		try {
			while((c=fin.read(buffer, 0, buffer.length)) != -1) {
				
				rpc.putBlock(buffer);
			}
		}
		catch(IOException e) {
			LOG.error("Writer error: " + e.toString());
		}
	}
}
