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
	implements Serializable, Comparable<HrfsRing>
{
	private List<String> hosts;
	private long htime;

	/**
	 * Hrfs Rings are immutable structures, and new ones
	 * have their htime set to a monotinicly increasing value
	 * that determines priority.
	 */
	public HrfsRing(List<String> hosts)
	{
		this.htime = System.nanoTime();
	}

	/**
	 * Returns the hash ring creation time for this hash ring.
	 * @return htime Hrfs Ring creation time.
	 */
	public long getHTime()
	{
		return this.htime;
	}

	/**
	 * Override of the compareTo for htime comparison, this compares
	 * to Hrfs Rings for priority.
	 * @param ring Hrfs Ring to compare against
	 */
	@Override
	public int compareTo(HrfsRing ring)
	{
		if(this.htime > ring.getHTime())
			return 1;
		else if(this.htime < ring.getHTime())
			return -1;

		return 0;
	}
}
