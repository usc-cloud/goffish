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
import java.util.concurrent.*;

import edu.usc.goffish.gofs.graph.*;

class StreamPartitioningWorker implements Callable<Object> {

	private final List<IStreamPartition> _partitions;
	private final IObjectiveFunction _objectiveFunction;
	private final Iterator<? extends IIdentifiableVertex> _vertexStream;
	
	public StreamPartitioningWorker(StreamPartitioningMaster master, Iterable<? extends IIdentifiableVertex> stream) {
		if (master == null) {
			throw new IllegalArgumentException();
		}
		if (stream == null) {
			throw new IllegalArgumentException();
		}
		
		_partitions = master.getPartitions();
		_objectiveFunction = master.getObjectiveFunction();
		_vertexStream = stream.iterator();
		
		if (_vertexStream == null) {
			throw new IllegalArgumentException();
		}
	}

	@Override
	public Object call() {
		while (_vertexStream.hasNext() && !Thread.interrupted()) {
			// choose partition and assign vertex
			IIdentifiableVertex vertex = _vertexStream.next();
			_objectiveFunction.choosePartition(vertex, _partitions).addVertex(vertex);
		}
		
		return null;
	}
}
