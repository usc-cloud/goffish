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

import it.unimi.dsi.fastutil.ints.*;
import it.unimi.dsi.fastutil.longs.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import edu.usc.goffish.gofs.formats.metis.*;
import edu.usc.goffish.gofs.graph.*;
import edu.usc.goffish.gofs.util.partitioning.*;

public final class StreamPartitioningMaster {

	static final int INVALID_PARTITION = -1;

	private final int _numPartitions;
	private final int _numVertices;
	private final List<IStreamPartition> _partitions;

	private final IObjectiveFunction _objectiveFunction;

	private final ForkJoinPool _pool;
	
	private final ConcurrentHashMap<Long, Integer> _vertexPartitions;
	private final AtomicInteger[] _partitionSizes;

	private final List<? extends Iterable<? extends IIdentifiableVertex>> _streams;

	private boolean _partitioned;

	public StreamPartitioningMaster(int numVertices, int numPartitions, IObjectiveFunction objectiveFunction, Iterable<? extends IIdentifiableVertex> streams) {
		this(numVertices, numPartitions, objectiveFunction, Collections.singletonList(streams));
	}
	
	public StreamPartitioningMaster(int numVertices, int numPartitions, IObjectiveFunction objectiveFunction, List<? extends Iterable<? extends IIdentifiableVertex>> streams) {
		if (numVertices < 1) {
			throw new IllegalArgumentException();
		}
		if (numPartitions < 1) {
			throw new IllegalArgumentException();
		}
		if (objectiveFunction == null) {
			throw new IllegalArgumentException();
		}
		if (streams == null || streams.isEmpty()) {
			throw new IllegalArgumentException();
		}

		_numPartitions = numPartitions;
		_numVertices = numVertices;
		_objectiveFunction = objectiveFunction;

		_pool = new ForkJoinPool();
		
		_vertexPartitions = new ConcurrentHashMap<>(numVertices, 1f, Math.min(_pool.getParallelism(), streams.size()));
		_partitionSizes = new AtomicInteger[_numPartitions];
		for (int i = 0; i < _partitionSizes.length; i++) {
			_partitionSizes[i] = new AtomicInteger(0);
		}

		ArrayList<IStreamPartition> partitions = new ArrayList<>(_numPartitions);
		for (int i = 0; i < _numPartitions; i++) {
			partitions.add(new StreamPartition(i));
		}
		_partitions = Collections.unmodifiableList(partitions);
		_streams = new LinkedList<>(streams);
		
		_partitioned = false;
	}

	public int getNumPartitions() {
		return _numPartitions;
	}

	public int getNumVertices() {
		return _numVertices;
	}

	public IObjectiveFunction getObjectiveFunction() {
		return _objectiveFunction;
	}

	List<IStreamPartition> getPartitions() {
		return _partitions;
	}
	
	public IPartitioning run() throws InterruptedException, ExecutionException {
		if (_partitioned) {
			// already partitioned
			throw new IllegalStateException();
		}
		_partitioned = true;
		
		if (_streams.size() > 1) {
			// set up parallel work
			List<StreamPartitioningWorker> computeList = new ArrayList<>(_streams.size());
			for (Iterable<? extends IIdentifiableVertex> stream : _streams) {
				computeList.add(new StreamPartitioningWorker(this, stream));
			}
	
			// do parallel work
			List<Future<Object>> results = _pool.invokeAll(computeList);
	
			// force any exceptions
			for (Future<Object> result : results) {
				result.get();
			}
		} else {
			// do serial work
			assert(_streams.size() == 1);
			new StreamPartitioningWorker(this, _streams.get(0)).call();
		}
	
		// convert to partitioning format
		IntSet partitions = new IntArraySet(); // assumes not many partitions
		for (IStreamPartition partition : _partitions) {
			partitions.add(partition.getId());
		}

		return new MetisPartitioning(partitions, new Long2IntOpenHashMap(_vertexPartitions));
	}

	private final class StreamPartition implements IStreamPartition {

		private final int _partitionIdx;

		public StreamPartition(int partitionIndex) {
			_partitionIdx = partitionIndex;
		}

		@Override
		public int getId() {
			return _partitionIdx + 1;
		}
		
		@Override
		public boolean containsVertex(IIdentifiableVertex vertex) {
			Integer partitionId = _vertexPartitions.get(vertex.getId());
			return partitionId != null && partitionId.intValue() == getId();
		}

		@Override
		public int size() {
			return _partitionSizes[_partitionIdx].get();
		}

		@Override
		public void addVertex(IIdentifiableVertex vertex) {
			// this check may fail during partitioning since we don't have a
			// memory barrier for concurrent accesses, but it will prevent
			// changing partitions after partitioning is complete
			if (_vertexPartitions.containsKey(vertex.getId())) {
				// vertex has already been partitioned
				throw new IllegalStateException();
			}

			_vertexPartitions.put(vertex.getId(), getId());
			_partitionSizes[_partitionIdx].incrementAndGet();
		}
		
		@Override
		public String toString() {
			return "StreamPartition@" + _partitionIdx + "[size=" +  _partitionSizes[_partitionIdx].get() + "]";
		}
	}
}
