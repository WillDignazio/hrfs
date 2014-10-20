/**
 * Copyright © 2014
 * Block Party Filesystem Configuration
 *
 * @author Will Dignazio <wdignazio@gmail.com>
 * @file BpfsConfiguration.java
 */
package edu.rit.cs;

import org.apache.hadoop.classification.InterfaceAudience;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

@InterfaceAudience.Private
public class BpfsConfiguration extends Configuration
{
	static
	{
		Configuration.addDefaultResource("bpfs-default.xml");
		Configuration.addDefaultResource("bpfs-site.xml");
	}

	/* 
	 * Stub that just wanders into the class, making sure that
	 * the static methods get called.
	 */
	public static void init() { }

	public BpfsConfiguration()
	{
		super();
	}

	public BpfsConfiguration(boolean loadDefaults)
	{
		super(loadDefaults);
	}

	public BpfsConfiguration(Configuration conf)
	{
		super(conf);
	}

	public static void main(String[] args) {
		init();
		Configuration.dumpDeprecatedKeys();
	}
}
