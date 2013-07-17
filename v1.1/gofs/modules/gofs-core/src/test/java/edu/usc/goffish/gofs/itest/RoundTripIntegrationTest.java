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
package edu.usc.goffish.gofs.itest;

import it.unimi.dsi.fastutil.longs.*;

import java.io.*;
import java.nio.file.*;

import junit.framework.*;

import edu.usc.goffish.gofs.*;
import edu.usc.goffish.gofs.partition.*;
import edu.usc.goffish.gofs.slice.*;
import edu.usc.goffish.gofs.util.*;

public class RoundTripIntegrationTest extends TestCase implements IntegrationTest {

	private static final IPartition _partition;
	static {
		long time = System.currentTimeMillis();
		ISubgraphTemplate graph = GoogleGraphLoader.constructGoogleGraph();
		System.out.println("graph load: " + (System.currentTimeMillis() - time) / 1000.0);

		time = System.currentTimeMillis();
		_partition = new BasePartition(1, graph, PropertySet.EmptyPropertySet, PropertySet.EmptyPropertySet, Long2IntMaps.EMPTY_MAP, new TestSubgraphFactory());
		System.out.println("graph partitioning: " + (System.currentTimeMillis() - time) / 1000.0);
	}

	public void testJavaSerializationRoundtrip() throws IOException {
		Path testDir = Files.createTempDirectory("test");
		try {
			SliceManager sliceManager = new SliceManager(new JavaSliceSerializer(), new FileStorageManager(testDir));
			doSerializationRoundtrip("java", sliceManager);
		} finally {
			FileHelper.delete(testDir.toFile());
		}
	}

	public void testKryoSerializationRoundtrip() throws IOException {
		Path testDir = Files.createTempDirectory("test");
		try {
			SliceManager sliceManager = new SliceManager(new KryoSliceSerializer(), new FileStorageManager(testDir));
			doSerializationRoundtrip("kryo", sliceManager);
		} finally {
			FileHelper.delete(testDir.toFile());
		}
	}

	public void doSerializationRoundtrip(String serializationType, SliceManager sliceManager) throws IOException {
		long time = System.currentTimeMillis();
		long bytes = sliceManager.writePartition(_partition);
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
