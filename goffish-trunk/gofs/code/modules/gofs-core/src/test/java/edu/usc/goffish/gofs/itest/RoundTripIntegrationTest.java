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

package edu.usc.goffish.gofs.itest;

import java.io.*;
import java.nio.file.*;
import java.util.*;

import org.apache.commons.io.*;

import junit.framework.*;

import edu.usc.goffish.gofs.*;
import edu.usc.goffish.gofs.partition.*;
import edu.usc.goffish.gofs.slice.*;

public class RoundTripIntegrationTest extends TestCase implements IntegrationTest {

	private static final IPartition _partition;
	static {
		long time = System.currentTimeMillis();
		ISubgraphTemplate<TemplateVertex, TemplateEdge> graph = GoogleGraphLoader.constructGoogleGraph();
		System.out.println("graph load: " + (System.currentTimeMillis() - time) / 1000.0);

		time = System.currentTimeMillis();
		_partition = new Partition(1, graph.isDirected(), Collections.singletonList(new TestSubgraph(1, graph, PropertySet.EmptyPropertySet, PropertySet.EmptyPropertySet)), PropertySet.EmptyPropertySet, PropertySet.EmptyPropertySet);
		System.out.println("graph partitioning: " + (System.currentTimeMillis() - time) / 1000.0);
	}

	public void testJavaSerializationRoundtrip() throws IOException {
		Path testDir = Files.createTempDirectory("test");
		try {
			ISliceManager sliceManager = SliceManager.create(new JavaSliceSerializer(), new FileStorageManager(testDir));
			doSerializationRoundtrip("java", sliceManager);
		} finally {
			FileUtils.deleteQuietly(testDir.toFile());
		}
	}

	public void testKryoSerializationRoundtrip() throws IOException {
		Path testDir = Files.createTempDirectory("test");
		try {
			ISliceManager sliceManager = SliceManager.create(new KryoSliceSerializer(), new FileStorageManager(testDir));
			doSerializationRoundtrip("kryo", sliceManager);
		} finally {
			FileUtils.deleteQuietly(testDir.toFile());
		}
	}

	public void doSerializationRoundtrip(String serializationType, ISliceManager sliceManager) throws IOException {
		long time = System.currentTimeMillis();
		long bytes = sliceManager.writePartitionTemplate(_partition);
		System.out.println(serializationType + " serialization: " + (System.currentTimeMillis() - time) / 1000.0 + "s , " + bytes / 1000.0 + "kb");

		time = System.currentTimeMillis();
		IPartition actualPartition = sliceManager.readPartition();
		System.out.println(serializationType + " deserialization: " + (System.currentTimeMillis() - time) / 1000.0 + "s");

		assertEquals(_partition, actualPartition);

		sliceManager.deletePartition();
	}

	protected void assertEquals(IPartition expectedPartition, IPartition actualPartition) {
		assertNotNull(expectedPartition);
		assertNotNull(actualPartition);

		if (expectedPartition != null && actualPartition != null) {
			assertEquals(expectedPartition.getId(), actualPartition.getId());
			assertEquals(expectedPartition.isDirected(), actualPartition.isDirected());

			for (ISubgraph expectedSubgraph : expectedPartition) {
				ISubgraph actualSubgraph = actualPartition.getSubgraph(expectedSubgraph.getId());
				assertEquals(expectedSubgraph, actualSubgraph);
			}
		}
	}

	protected void assertEquals(ISubgraph expectedSubgraph, ISubgraph actualSubgraph) {
		assertNotNull(expectedSubgraph);
		assertNotNull(actualSubgraph);

		if (expectedSubgraph != null && actualSubgraph != null) {
			assertEquals(expectedSubgraph.getId(), actualSubgraph.getId());
			assertEquals(expectedSubgraph.getTemplate().numVertices(), actualSubgraph.getTemplate().numVertices());
			assertEquals(expectedSubgraph.getTemplate().numEdges(), actualSubgraph.getTemplate().numEdges());
			assertEquals(expectedSubgraph.isDirected(), actualSubgraph.isDirected());
			assertEquals(expectedSubgraph.getTemplate().isDirected(), actualSubgraph.getTemplate().isDirected());
			assertEquals(actualSubgraph.isDirected(), actualSubgraph.getTemplate().isDirected());

			// TODO: verify individual vertices/edges
		}
	}
}
