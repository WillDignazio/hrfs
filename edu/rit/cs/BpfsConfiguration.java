/**
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

	public BpfsConfiguration()
	{
		super();
	}
}
