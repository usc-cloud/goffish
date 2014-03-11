/*
*    Copyright 2013 University of Southern California
*
*  Licensed under the Apache License, Version 2.0 (the "License");
*  you may not use this file except in compliance with the License. 
*  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
*  Unless required by applicable law or agreed to in writing,
*  software distributed under the License is distributed on an
*  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
*  KIND, either express or implied.  See the License for the
*  specific language governing permissions and limitations
*  under the License.
*/

package edu.usc.goffish.gofs.partition.gml;

import java.io.*;
import java.nio.file.*;
import java.util.*;

import junit.framework.*;

import org.apache.commons.io.*;

import edu.usc.goffish.gofs.*;
import edu.usc.goffish.gofs.formats.gml.*;
import edu.usc.goffish.gofs.formats.metis.*;
import edu.usc.goffish.gofs.partition.components.*;

public class GMLPartitionerTest extends TestCase {

	private GMLPartitioner _partitioner;

	public void setUp() throws IOException {
		_partitioner = new GMLPartitioner(MetisPartitioning.read(ClassLoader.getSystemResourceAsStream("simple_partition.txt")));
	}

	public void testPartitioner() throws IOException {
		Path outputDir = Files.createTempDirectory("gofs_test");
		try {
			_partitioner.partitionTemplate(ClassLoader.getSystemResourceAsStream("simple_graph_template.gml"), outputDir, "partition_", "_template.gml");
			// TODO: test instance partitions

			Path p1_t = outputDir.resolve("partition_1_template.gml");
			// Path p1_i1 = _outputDir.resolve("partition_1_instance1.gml");
			GMLPartition p1 = GMLPartition.parseGML(1, new WCCComponentizer(), Files.newInputStream(p1_t), Collections.<InputStream>emptyList());

			ISubgraph s10 = p1.iterator().next();

			assertTrue(s10.getTemplate().isDirected());
			assertTrue(s10.containsVertex(1));
			assertTrue(s10.containsVertex(2));
			assertTrue(s10.containsVertex(3));
			assertTrue(s10.getVertex(1).containsOutEdgeTo(s10.getVertex(2)));
			assertFalse(s10.getVertex(2).containsOutEdgeTo(s10.getVertex(1)));
			assertFalse(s10.getVertex(1).isRemote());
			assertFalse(s10.getVertex(2).isRemote());
			assertTrue(s10.getVertex(3).isRemote());
			assertEquals(2, s10.getVertex(3).getRemotePartitionId());

			Path p2_t = outputDir.resolve("partition_2_template.gml");
			// Path p2_i1 = _outputDir.resolve("partition_2_instance1.gml");
			GMLPartition p2 = GMLPartition.parseGML(1, new WCCComponentizer(), Files.newInputStream(p2_t), Collections.<InputStream>emptyList());

			ISubgraph s20 = p2.iterator().next();

			assertTrue(s20.isDirected());
			assertFalse(s20.containsVertex(1));
			assertFalse(s20.containsVertex(2));
			assertTrue(s20.containsVertex(3));
			assertTrue(s20.containsVertex(4));
			assertTrue(s20.getVertex(3).containsOutEdgeTo(s20.getVertex(4)));
			assertFalse(s20.getVertex(4).containsOutEdgeTo(s20.getVertex(3)));
			assertFalse(s20.getVertex(3).isRemote());
			assertFalse(s20.getVertex(4).isRemote());
		} finally {
			FileUtils.deleteQuietly(outputDir.toFile());
		}
	}
}
