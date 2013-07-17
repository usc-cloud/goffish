/*
*  Licensed to the Apache Software Foundation (ASF) under one
*  or more contributor license agreements.  See the NOTICE file
*  distributed with this work for additional information
*  regarding copyright ownership.  The ASF licenses this file
*  to you under the Apache License, Version 2.0 (the
*  "License"); you may not use this file except in compliance
*  with the License.  You may obtain a copy of the License at
*
*   http://www.apache.org/licenses/LICENSE-2.0
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

import edu.usc.goffish.gofs.formats.gml.*;
import edu.usc.goffish.gofs.formats.metis.*;
import edu.usc.goffish.gofs.util.*;

import junit.framework.*;

public class GMLPartitionerTest  extends TestCase {
	
	private GMLPartitioner _partitioner;
	
	public void setUp() throws IOException {
		_partitioner = new GMLPartitioner(MetisPartitioning.read(ClassLoader.getSystemResourceAsStream("simple_partition.txt")));
	}

	public void testPartitioner() throws IOException {
		Path outputDir = Files.createTempDirectory("gofs_test");
		try {
			_partitioner.partitionTemplate(ClassLoader.getSystemResourceAsStream("simple_graph_template.gml"), outputDir, "partition_" , "_template.gml");
			// TODO: test instance partitions
			
			Path p1_t = outputDir.resolve("partition_1_template.gml");
			//Path p1_i1 = _outputDir.resolve("partition_1_instance1.gml");
			GMLPartition p1 = GMLPartition.parseGML(1, Files.newInputStream(p1_t), Collections.<InputStream>emptyList());
	
			assertTrue(p1.getSubgraph(0).getTemplate().isDirected());
			assertTrue(p1.getSubgraph(0).containsVertex(1));
			assertTrue(p1.getSubgraph(0).containsVertex(2));
			assertTrue(p1.getSubgraph(0).containsVertex(3));
			assertTrue(p1.getSubgraph(0).getVertex(1).containsOutEdgeTo(p1.getSubgraph(0).getVertex(2)));
			assertFalse(p1.getSubgraph(0).getVertex(2).containsOutEdgeTo(p1.getSubgraph(0).getVertex(1)));
			assertFalse(p1.getSubgraph(0).isRemoteVertex(1));
			assertFalse(p1.getSubgraph(0).isRemoteVertex(2));
			assertTrue(p1.getSubgraph(0).isRemoteVertex(3));
			assertEquals(2, p1.getSubgraph(0).getPartitionForRemoteVertex(3));
	
			Path p2_t = outputDir.resolve("partition_2_template.gml");
			//Path p2_i1 = _outputDir.resolve("partition_2_instance1.gml");
			GMLPartition p2 = GMLPartition.parseGML(1, Files.newInputStream(p2_t), Collections.<InputStream>emptyList());
	
			assertTrue(p2.getSubgraph(0).isDirected());
			assertFalse(p2.getSubgraph(0).containsVertex(1));
			assertFalse(p2.getSubgraph(0).containsVertex(2));
			assertTrue(p2.getSubgraph(0).containsVertex(3));
			assertTrue(p2.getSubgraph(0).containsVertex(4));
			assertTrue(p2.getSubgraph(0).getVertex(3).containsOutEdgeTo(p2.getSubgraph(0).getVertex(4)));
			assertFalse(p2.getSubgraph(0).getVertex(4).containsOutEdgeTo(p2.getSubgraph(0).getVertex(3)));
			assertFalse(p2.getSubgraph(0).isRemoteVertex(3));
			assertFalse(p2.getSubgraph(0).isRemoteVertex(4));
		} finally {
			FileHelper.delete(outputDir.toFile());
		}
	}
}
