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

package edu.usc.goffish.gofs.namenode;

import it.unimi.dsi.fastutil.ints.*;

import java.io.*;
import java.net.*;

import junit.framework.*;
import edu.usc.goffish.gofs.*;
import edu.usc.goffish.gofs.slice.*;

public class NameNodeTest extends TestCase {

	private static final URI URI0 = URI.create("file:///");
	private static final URI URI1 = URI.create("file:///tmp1");
	private static final URI URI2 = URI.create("file:///tmp2");
	private static final URI URI3 = URI.create("file:///tmp3");
	private static final URI URI4 = URI.create("file:///tmp4");

	public void testLocalNameNode() throws IOException {
		doTestNameNode(new LocalNameNode());
	}

	public void testRemoteNameNode() throws IOException {
		NameNodeServer _server = new NameNodeServer(URI.create("http://localhost:9998"));
		_server.start();
		try {
			doTestNameNode(new RemoteNameNode(URI.create("http://localhost:9998")));
		} finally {
			_server.stop();
		}
	}

	public void doTestNameNode(IInternalNameNode nameNode) throws IOException {
		// test data nodes
		assertTrue(nameNode.getDataNodes().isEmpty());
		nameNode.addDataNode(URI.create("file:///test/"));
		assertEquals(1, nameNode.getDataNodes().size());
		assertTrue(nameNode.getDataNodes().contains(URI.create("file:///test/")));
		
		assertEquals(null, nameNode.getSerializer());
		nameNode.setSerializer(KryoSliceSerializer.class);
		assertEquals(KryoSliceSerializer.class.getName(), nameNode.getSerializer().getClass().getName());
		
		doTestPartitionDirectory(nameNode.getPartitionDirectory());
	}
	
	public void doTestPartitionDirectory(IPartitionDirectory partitionDirectory) throws IOException {
		partitionDirectory.putPartitionMapping("graph1", 1, URI1);
		partitionDirectory.putPartitionMapping("graph1", Integer.MAX_VALUE, URI2);
		partitionDirectory.putPartitionMapping("graph2", 1, URI3);

		// test getPartitions
		IntCollection partitions1 = partitionDirectory.getPartitions("graph1");
		assertNotNull(partitions1);
		assertTrue(partitions1.contains(1));
		assertTrue(partitions1.contains(Integer.MAX_VALUE));
		assertFalse(partitions1.contains(3));

		IntCollection partitions2 = partitionDirectory.getPartitions("graph2");
		assertNotNull(partitions2);
		assertTrue(partitions2.contains(1));
		assertFalse(partitions2.contains(Integer.MAX_VALUE));
		assertFalse(partitions2.contains(3));

		IntCollection partitions3 = partitionDirectory.getPartitions("graph3");
		assertNull(partitions3);

		// test getMatchingPartitions
		IntCollection matching = partitionDirectory.getMatchingPartitions("graph1", URI0);
		assertNotNull(matching);
		assertTrue(matching.contains(1));
		assertFalse(matching.contains(2));
		assertFalse(matching.contains(3));
		assertFalse(matching.contains(4));
		assertTrue(matching.contains(Integer.MAX_VALUE));
		
		// test getMatchingPartitions
		matching = partitionDirectory.getMatchingPartitions("graph1", URI1);
		assertNotNull(matching);
		assertTrue(matching.contains(1));
		assertFalse(matching.contains(2));
		assertFalse(matching.contains(3));
		assertFalse(matching.contains(4));
		assertFalse(matching.contains(Integer.MAX_VALUE));

		// test getPartitionMapping
		assertEquals(URI1, partitionDirectory.getPartitionMapping("graph1", 1));
		assertEquals(URI2, partitionDirectory.getPartitionMapping("graph1", Integer.MAX_VALUE));
		assertEquals(URI3, partitionDirectory.getPartitionMapping("graph2", 1));
		assertNull(partitionDirectory.getPartitionMapping("graph3", 1));
		assertNull(partitionDirectory.getPartitionMapping("graph1", 3));

		// test hasPartitionMapping
		assertTrue(partitionDirectory.hasPartitionMapping("graph1", 1));
		assertTrue(partitionDirectory.hasPartitionMapping("graph1", Integer.MAX_VALUE));
		assertTrue(partitionDirectory.hasPartitionMapping("graph2", 1));
		assertFalse(partitionDirectory.hasPartitionMapping("graph3", 1));
		assertFalse(partitionDirectory.hasPartitionMapping("graph1", 3));

		// test putPartitionMapping
		assertFalse(partitionDirectory.hasPartitionMapping("graph3", 1));
		partitionDirectory.putPartitionMapping("graph3", 1, URI4);
		assertTrue(partitionDirectory.hasPartitionMapping("graph3", 1));
		assertEquals(URI4, partitionDirectory.getPartitionMapping("graph3", 1));

		// test removePartitionMapping
		assertTrue(partitionDirectory.hasPartitionMapping("graph1", Integer.MAX_VALUE));
		partitionDirectory.removePartitionMapping("graph1", Integer.MAX_VALUE);
		assertFalse(partitionDirectory.hasPartitionMapping("graph1", Integer.MAX_VALUE));
		assertNull(partitionDirectory.getPartitionMapping("graph1", Integer.MAX_VALUE));
	}

}
