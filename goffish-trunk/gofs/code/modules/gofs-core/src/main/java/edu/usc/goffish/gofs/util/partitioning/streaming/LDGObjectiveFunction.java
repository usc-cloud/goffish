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

import java.util.*;

import edu.usc.goffish.gofs.graph.*;

public class LDGObjectiveFunction implements IObjectiveFunction {
	
	private final BalancedObjectiveFunction _balancedTieBreaker;
	private final ArrayList<IStreamPartition> _ties;
	private double _capacityConstraint;
	
	public LDGObjectiveFunction() {
		_balancedTieBreaker = new BalancedObjectiveFunction();
		_ties = new ArrayList<>();
		_capacityConstraint = 1;
	}

	public BalancedObjectiveFunction getBalanced() {
		return _balancedTieBreaker;
	}
	
	@Override
	public void reset(int numVertices, int numPartitions) {
		_capacityConstraint = (int)((1.01f * numVertices) / numPartitions);
	}
	
	@Override
	public IStreamPartition choosePartition(IIdentifiableVertex vertex, List<IStreamPartition> partitions) {
		assert(vertex != null && partitions != null);
		
		_ties.ensureCapacity(partitions.size());
		
		_ties.clear();
		double score;
		double maxScore = Double.NEGATIVE_INFINITY;
		
		for (IStreamPartition partition : partitions) {
			score = edgeConnectivity(vertex, partition) * (1 - (partition.size() / _capacityConstraint));
			if (score > maxScore) {
				maxScore = score;
				_ties.clear();
				_ties.add(partition);
			} else if (score == maxScore) {
				_ties.add(partition);
			}
		}
		
		assert(!_ties.isEmpty());
		
		if (_ties.size() == 1) {
			return _ties.get(0);
		} else {
			return _balancedTieBreaker.choosePartition(vertex, _ties);
		}
	}

	private static long edgeConnectivity(IIdentifiableVertex vertex, IStreamPartition partition) {
		long connectivityCount = 0;
		for (IEdge edge : vertex.outEdges()) {
			if (partition.containsVertex((IIdentifiableVertex)edge.getSink(vertex))) {
				connectivityCount++;
			}
		}
		
		return connectivityCount;
	}
}
