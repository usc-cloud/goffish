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
package edu.usc.goffish.gofs.namenode;

import java.io.*;
import java.net.*;
import java.nio.file.*;
import java.util.*;

import junit.framework.*;

import edu.usc.goffish.gofs.*;
import edu.usc.goffish.gofs.util.*;

public class NameNodeTest extends TestCase {

	private static final URI URI1 = URI.create("file:///tmp1");
	private static final URI URI2 = URI.create("file:///tmp2");
	private static final URI URI3 = URI.create("file:///tmp3");
	private static final URI URI4 = URI.create("file:///tmp4");

	public void testLocalNameNode() throws IOException {
		doTestNameNode(new LocalNameNode());
	}

	public void testFileNameNode() throws IOException {
		Path tmpNameNodePath = Files.createTempFile("namenode", "");
		try {
			doTestNameNode(new FileNameNode(tmpNameNodePath));
		} finally {
			FileHelper.delete(tmpNameNodePath.toFile());
		}
	}

	public void testRemoteNameNode() throws IOException {
		NameNodeServer _server = new NameNodeServer("localhost", 9998);
		_server.start();
		try {
			doTestNameNode(new RemoteNameNode("localhost", 9998));
		} finally {
			_server.stop();
		}
	}

	public void doTestNameNode(INameNode nameNode) throws IOException {
		nameNode.putPartitionMapping("graph1", 1, URI1);
		nameNode.putPartitionMapping("graph1", Integer.MAX_VALUE, URI2);
		nameNode.putPartitionMapping("graph2", 1, URI3);

		// test getPartitions
		List<Integer> partitions1 = nameNode.getPartitions("graph1");
		assertNotNull(partitions1);
		assertTrue(partitions1.contains(1));
		assertTrue(partitions1.contains(Integer.MAX_VALUE));
		assertFalse(partitions1.contains(3));

		List<Integer> partitions2 = nameNode.getPartitions("graph2");
		assertNotNull(partitions2);
		assertTrue(partitions2.contains(1));
		assertFalse(partitions2.contains(Integer.MAX_VALUE));
		assertFalse(partitions2.contains(3));

		List<Integer> partitions3 = nameNode.getPartitions("graph3");
		assertNull(partitions3);

		// test getMatchingPartitions
		List<Integer> matching = nameNode.getMatchingPartitions("graph1", URI.create(URI1.getPath()));
		assertNotNull(matching);
		assertTrue(matching.contains(1));
		assertFalse(matching.contains(Integer.MAX_VALUE));

		// test getPartitionMapping
		assertEquals(URI1, nameNode.getPartitionMapping("graph1", 1));
		assertEquals(URI2, nameNode.getPartitionMapping("graph1", Integer.MAX_VALUE));
		assertEquals(URI3, nameNode.getPartitionMapping("graph2", 1));
		assertNull(nameNode.getPartitionMapping("graph3", 1));
		assertNull(nameNode.getPartitionMapping("graph1", 3));

		// test hasPartitionMapping
		assertTrue(nameNode.hasPartitionMapping("graph1", 1));
		assertTrue(nameNode.hasPartitionMapping("graph1", Integer.MAX_VALUE));
		assertTrue(nameNode.hasPartitionMapping("graph2", 1));
		assertFalse(nameNode.hasPartitionMapping("graph3", 1));
		assertFalse(nameNode.hasPartitionMapping("graph1", 3));

		// test putPartitionMapping
		assertFalse(nameNode.hasPartitionMapping("graph3", 1));
		nameNode.putPartitionMapping("graph3", 1, URI4);
		assertTrue(nameNode.hasPartitionMapping("graph3", 1));
		assertEquals(URI4, nameNode.getPartitionMapping("graph3", 1));

		// test removePartitionMapping
		assertTrue(nameNode.hasPartitionMapping("graph1", Integer.MAX_VALUE));
		nameNode.removePartitionMapping("graph1", Integer.MAX_VALUE);
		assertFalse(nameNode.hasPartitionMapping("graph1", Integer.MAX_VALUE));
		assertNull(nameNode.getPartitionMapping("graph1", Integer.MAX_VALUE));
	}

}
