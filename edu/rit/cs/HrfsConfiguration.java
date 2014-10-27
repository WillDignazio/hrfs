/**
 * Copyright © 2014
 * Hadoop Replicating Filesystem Configuration
 *
 * @author Will Dignazio <wdignazio@gmail.com>
 * @file HrfsConfiguration.java
 */
package edu.rit.cs;

import org.apache.hadoop.classification.InterfaceAudience;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

@InterfaceAudience.Private
public class HrfsConfiguration extends Configuration
{
	static
	{
		Configuration.addDefaultResource("hrfs-default.xml");
		Configuration.addDefaultResource("hrfs-site.xml");
	}

	/* 
	 * Stub that just wanders into the class, making sure that
	 * the static methods get called.
	 */
	public static void init() { }

	public HrfsConfiguration()
	{
		super();
	}

	public HrfsConfiguration(boolean loadDefaults)
	{
		super(loadDefaults);
	}

	public HrfsConfiguration(Configuration conf)
	{
		super(conf);
	}

	public static void main(String[] args) {
		init();
		Configuration.dumpDeprecatedKeys();
	}
}
