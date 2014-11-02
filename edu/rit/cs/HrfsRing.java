/**
 * Copyright © 2014
 * Hrfs Ring Object and Utilities
 *
 * @file HrfsRing.java
 * @author Will Dignazio <wdignazio@gmail.com>
 */
package edu.rit.cs;

import java.util.List;
import java.util.HashMap;
import java.io.Serializable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public final class HrfsRing
	implements Serializable
{
	private List<String> hosts;

	/**
	 * Hrfs Rings are immutable structures, and new ones
	 * have their htime set to a monotinicly increasing value
	 * that determines priority.
	 */
	public HrfsRing(List<String> hosts)
	{
		this.hosts = hosts;
	}
}
