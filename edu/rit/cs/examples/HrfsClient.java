package edu.rit.cs.examples;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

import edu.rit.cs.*;

public class HrfsClient
{
	public static void main(String[] args)
		throws Exception
	{
		FileSystem fs;
		HrfsConfiguration conf;

		conf = new HrfsConfiguration();
		fs = FileSystem.get(conf);
	}
}
