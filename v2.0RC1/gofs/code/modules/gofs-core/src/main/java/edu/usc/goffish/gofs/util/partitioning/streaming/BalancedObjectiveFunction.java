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

public class BalancedObjectiveFunction implements IObjectiveFunction {

	@Override
	public void reset(int numVertices, int numPartitions) {
	}
	
	@Override
	public IStreamPartition choosePartition(IIdentifiableVertex vertex, List<IStreamPartition> partitions) {		
		IStreamPartition minSizePartition = null;
		int minSize = Integer.MAX_VALUE;
		int size;
		
		for (IStreamPartition partition : partitions) {
			size = partition.size();
			if (size < minSize) {
				minSize = size;
				minSizePartition = partition;
			}
		}
		
		assert(minSizePartition != null);
		return minSizePartition;
	}
}
