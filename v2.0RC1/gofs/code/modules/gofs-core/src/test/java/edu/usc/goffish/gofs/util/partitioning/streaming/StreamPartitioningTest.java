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

package edu.usc.goffish.gofs.util.partitioning.streaming;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import junit.framework.*;
import edu.usc.goffish.gofs.formats.metis.*;
import edu.usc.goffish.gofs.util.partitioning.*;

public class StreamPartitioningTest extends TestCase {

	private static final String METIS_INPUT = "metis.input";
	private static final int NUM_PARTITIONS = 2;
	
	private MetisGraph _graph;
	
	@Override
	public void setUp() throws IOException {
		_graph = MetisGraph.read(ClassLoader.getSystemResourceAsStream(METIS_INPUT));
	}
	
	public void testLDG() throws InterruptedException, ExecutionException {
		doStreamPartitioning(new LDGObjectiveFunction(), NUM_PARTITIONS);
	}
	
	public void testBalanced() throws InterruptedException, ExecutionException {
		doStreamPartitioning(new BalancedObjectiveFunction(), NUM_PARTITIONS);
	}
	
	public void testBalanceBig() throws InterruptedException, ExecutionException {
		doStreamPartitioning(new BalanceBigObjectiveFunction(10), NUM_PARTITIONS);
	}
	
	public void testHashing() throws InterruptedException, ExecutionException {
		doStreamPartitioning(new HashingObjectiveFunction(), NUM_PARTITIONS);
	}
	
	private void doStreamPartitioning(IObjectiveFunction objectiveFunction, int numPartitions) throws InterruptedException, ExecutionException {
		StreamPartitioningMaster partitioner = new StreamPartitioningMaster(_graph.numVertices(), numPartitions, objectiveFunction, _graph.vertices());
		IPartitioning partitioning = partitioner.run();
		
		assertEquals(partitioning.getPartitions().size(), numPartitions);
		assertEquals(partitioning.size(), _graph.numVertices());
		for (Map.Entry<Long, Integer> entry : partitioning.entrySet()) {
			assertTrue(partitioning.getPartitions().contains(entry.getValue()));
			assertTrue(_graph.containsVertex(entry.getKey()));
		}
	}
}
