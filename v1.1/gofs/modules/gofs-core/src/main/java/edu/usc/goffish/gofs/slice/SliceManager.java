/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law
 * or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */

package edu.usc.goffish.gofs.slice;

import it.unimi.dsi.fastutil.longs.*;

import java.io.*;
import java.util.*;

import edu.usc.goffish.gofs.*;
import edu.usc.goffish.gofs.partition.*;
import edu.usc.goffish.gofs.slice.PartitionMetadataSlice.SubgraphInstancesInfo;

public class SliceManager implements ISliceManager {

	// all important meta data slice
	private final UUID _partitionMetadataSliceId;

	private boolean _metadataCacheValid;

	// cached meta data information
	private int _partitionId;
	private UUID _partitionSliceId;
	private Map<Long, SubgraphInstancesInfo> _subgraphInstancesInfo;

	private final ISliceSerializer _sliceSerializer;
	private final IStorageManager _storageManager;
	private final ISubgraphFactory _subgraphFactory;

	public SliceManager(ISliceSerializer sliceSerializer, IStorageManager storageManager) throws IOException {
		this(UUID.randomUUID(), sliceSerializer, storageManager);
	}

	public SliceManager(UUID partitionMetadataSliceId, ISliceSerializer sliceSerializer, IStorageManager storageManager) throws IOException {
		if (partitionMetadataSliceId == null) {
			throw new IllegalArgumentException();
		}
		if (sliceSerializer == null) {
			throw new IllegalArgumentException();
		}
		if (storageManager == null) {
			throw new IllegalArgumentException();
		}

		_partitionMetadataSliceId = partitionMetadataSliceId;
		_sliceSerializer = sliceSerializer;
		_storageManager = storageManager;
		_metadataCacheValid = false;

		readMetadata();

		// we're letting the self reference escape, but at least we're doing it last...
		_subgraphFactory = new SliceSubgraphFactory(this);
	}

	@Override
	public UUID getPartitionUUID() {
		return _partitionMetadataSliceId;
	}

	@Override
	public synchronized void invalidate() {
		_metadataCacheValid = false;
	}

	@SuppressWarnings("unchecked")
	private synchronized void readMetadata() throws IOException {
		if (_storageManager.hasReadStream(_partitionMetadataSliceId)) {
			PartitionMetadataSlice metadata = _sliceSerializer.deserialize(_storageManager.getReadStream(_partitionMetadataSliceId), PartitionMetadataSlice.class);

			_partitionId = metadata.PartitionId;
			_partitionSliceId = metadata.PartitionSlice;
			_subgraphInstancesInfo = metadata.SubgraphInstancesInfo;
		} else {
			_partitionId = BasePartition.INVALID_PARTITION;
			_partitionSliceId = null;
			_subgraphInstancesInfo = (Long2ObjectMap<SubgraphInstancesInfo>)Long2ObjectMaps.EMPTY_MAP;
		}

		_metadataCacheValid = true;
	}

	private synchronized long writeNewMetadata(int partitionId, UUID partitionTemplateSliceId, Map<Long, SubgraphInstancesInfo> subgraphInstancesInfo) throws IOException {
		if (!_metadataCacheValid) {
			// may not write metadata when cache is stale
			throw new IllegalStateException();
		}

		// in reality this is not atomic, but we need it to be as atomic as possible
		PartitionMetadataSlice metadataSlice = new PartitionMetadataSlice(_partitionMetadataSliceId, partitionId, partitionTemplateSliceId, subgraphInstancesInfo);
		try (OutputStream out = _storageManager.getWriteStream(metadataSlice.getId())) {
			return _sliceSerializer.serialize(metadataSlice, out);
		}
	}

	@Override
	public synchronized IPartition readPartition() throws IOException {
		if (!_metadataCacheValid) {
			readMetadata();
		}

		if (_partitionId == BasePartition.INVALID_PARTITION) {
			throw new FileNotFoundException();
		}
		assert(_partitionSliceId != null);

		// deserialize the slice
		PartitionSlice partitionSlice;
		try (InputStream in = _storageManager.getReadStream(_partitionSliceId)) {
			partitionSlice = _sliceSerializer.deserialize(in, PartitionSlice.class);
		}

		return new BasePartition(_partitionId, partitionSlice.IsDirected, partitionSlice.getSubgraphs(_subgraphFactory), partitionSlice.getVertexProperties(), partitionSlice.getEdgeProperties());
	}

	synchronized Iterable<ISubgraphInstance> readInstances(final boolean reverse, final ISubgraph subgraph, final long startTime, final long endTime, final PropertySet vertexProperties, final PropertySet edgeProperties) throws IOException {
		if (subgraph == null) {
			throw new IllegalArgumentException();
		}
		if (vertexProperties == null) {
			throw new IllegalArgumentException();
		}
		if (edgeProperties == null) {
			throw new IllegalArgumentException();
		}
		
		if (!_metadataCacheValid) {
			readMetadata();
		}

		if (_partitionId == BasePartition.INVALID_PARTITION) {
			throw new FileNotFoundException();
		}
		assert(_subgraphInstancesInfo != null);

		// find appropriate instance info
		final SubgraphInstancesInfo info = _subgraphInstancesInfo.get(subgraph.getId());

		return new Iterable<ISubgraphInstance>() {
			@Override
			public Iterator<ISubgraphInstance> iterator() {
				return new InstanceIterator(reverse, SliceManager.this, info, subgraph, startTime, endTime, vertexProperties, edgeProperties);
			}
		};
	}
	
	synchronized long getInstancesFirstTime(long subgraphId) throws IOException {
		if (!_metadataCacheValid) {
			readMetadata();
		}
		
		SubgraphInstancesInfo info = _subgraphInstancesInfo.get(subgraphId);
		if (info == null) {
			return Long.MIN_VALUE;
		}
		
		NavigableSet<PartitionMetadataSlice.InstanceInfo> instances = info.getInstances();
		if (instances.isEmpty()) {
			return Long.MIN_VALUE;
		}
		
		return instances.first().TimespanStart;
	}
	
	synchronized long getInstancesLastTime(long subgraphId) throws IOException {
		if (!_metadataCacheValid) {
			readMetadata();
		}
		
		SubgraphInstancesInfo info = _subgraphInstancesInfo.get(subgraphId);
		if (info == null) {
			return Long.MIN_VALUE;
		}
		
		NavigableSet<PartitionMetadataSlice.InstanceInfo> instances = info.getInstances();
		if (instances.isEmpty()) {
			return Long.MIN_VALUE;
		}
		
		return instances.last().TimespanEnd;
	}
	
	synchronized PartitionInstancesSlice readInstancesSlice(UUID sliceId) throws IOException {
		// TODO: cache this slice for at least 1 call
		
		PartitionInstancesSlice instancesPropertySlice;
		try (InputStream in = _storageManager.getReadStream(sliceId)) {
			instancesPropertySlice = _sliceSerializer.deserialize(in, PartitionInstancesSlice.class);
		}

		return instancesPropertySlice;
	}

	@Override
	public synchronized long writePartition(IPartition partition) throws IOException {
		if (partition == null) {
			throw new IllegalArgumentException();
		}

		if (!_metadataCacheValid) {
			readMetadata();
		}

		if (_partitionId != BasePartition.INVALID_PARTITION && partition.getId() != _partitionId) {
			throw new IllegalArgumentException();
		}

		// the ordering of every operation in this function is important to
		// guarantee consistency, be careful changing it

		long total = 0;

		// new meta data
		int partitionId = BasePartition.INVALID_PARTITION;
		UUID partitionSliceId = null;

		try {
			PartitionSlice partitionSlice = new PartitionSlice(partition);

			// update new meta data
			partitionId = partition.getId();
			partitionSliceId = partitionSlice.getId();

			// serialize partition
			try (OutputStream out = _storageManager.getWriteStream(partitionSlice.getId())) {
				total += _sliceSerializer.serialize(partitionSlice, out);
			}

			// serialize new meta data - this MUST be the last item, if this does not fail, there is no going back
			total += writeNewMetadata(partitionId, partitionSliceId, _subgraphInstancesInfo);
		} catch (Exception e) {
			// clean up
			if (partitionSliceId != null) {
				deleteSlice(partitionSliceId);
			}

			throw e;
		}

		// if we've made it here we must update the meta data at all costs

		// hold onto old metadata for deletion
		UUID oldPartitionSliceId = _partitionSliceId;

		// update meta data
		_partitionId = partitionId;
		_partitionSliceId = partitionSliceId;
		
		// delete old slices - no exceptions
		try {
			deleteSlice(oldPartitionSliceId);
		} catch (Exception e) {}

		return total;
	}
	
	@Override
	public synchronized long writePartition(ISerializablePartition partition) throws IOException {
		if (partition == null) {
			throw new IllegalArgumentException();
		}

		if (!_metadataCacheValid) {
			readMetadata();
		}

		if (_partitionId != BasePartition.INVALID_PARTITION && partition.getId() != _partitionId) {
			throw new IllegalArgumentException();
		}

		// the ordering of every operation in this function is important to
		// guarantee consistency, be careful changing it

		long total = 0;

		// new meta data
		int partitionId = BasePartition.INVALID_PARTITION;
		UUID partitionSliceId = null;
		Long2ObjectMap<SubgraphInstancesInfo> subgraphInstancesInfo = new Long2ObjectRBTreeMap<>();

		try {
			PartitionSlice partitionSlice = new PartitionSlice(partition);

			// update new meta data
			partitionId = partition.getId();
			partitionSliceId = partitionSlice.getId();

			// serialize partition
			try (OutputStream out = _storageManager.getWriteStream(partitionSlice.getId())) {
				total += _sliceSerializer.serialize(partitionSlice, out);
			}

			// serialize instances
			for (List<Map<Long, ? extends ISubgraphInstance>> groupedSubgraphInstances : partition.getSubgraphsInstances()) {
				for (ISubgraph subgraph : partition) {

					// update new meta data
					SubgraphInstancesInfo info = subgraphInstancesInfo.get(subgraph.getId());
					if (info == null) {
						info = new SubgraphInstancesInfo();
						subgraphInstancesInfo.put(subgraph.getId(), info);
					}

					// get instances for current subgraph
					ArrayList<ISubgraphInstance> instances = new ArrayList<>(groupedSubgraphInstances.size());
					for (Map<Long, ? extends ISubgraphInstance> subgraphInstance : groupedSubgraphInstances) {
						if (subgraphInstance.containsKey(subgraph.getId())) {
							instances.add(subgraphInstance.get(subgraph.getId()));
						}
					}

					// write slice for each vertex property
					for (Property property : subgraph.getVertexProperties()) {
						PartitionInstancesSlice instancesPropertySlice = new PartitionInstancesSlice(property, instances, true);

						// update new meta data
						info.mapSlice(instances, property.getName(), true, instancesPropertySlice.getId());

						// serialize instance
						try (OutputStream out = _storageManager.getWriteStream(instancesPropertySlice.getId())) {
							total += _sliceSerializer.serialize(instancesPropertySlice, out);
						}
					}

					// write slice for each vertex property
					for (Property property : subgraph.getEdgeProperties()) {
						PartitionInstancesSlice instancesPropertySlice = new PartitionInstancesSlice(property, instances, false);

						// update meta data
						info.mapSlice(instances, property.getName(), false, instancesPropertySlice.getId());

						// serialize instance
						try (OutputStream out = _storageManager.getWriteStream(instancesPropertySlice.getId())) {
							total += _sliceSerializer.serialize(instancesPropertySlice, out);
						}
					}
				}
			}

			// serialize new meta data - this MUST be the last item, if this does not fail, there is no going back
			total += writeNewMetadata(partitionId, partitionSliceId, subgraphInstancesInfo);
		} catch (Exception e) {
			// clean up
			if (partitionSliceId != null) {
				deleteSlice(partitionSliceId);
			}

			for (SubgraphInstancesInfo info : subgraphInstancesInfo.values()) {
				for (UUID sliceId : info.getAllSlices()) {
					deleteSlice(sliceId);
				}
			}

			throw e;
		}

		// if we've made it here we must update the meta data at all costs

		// hold onto old metadata for deletion
		UUID oldPartitionSliceId = _partitionSliceId;
		Map<Long, SubgraphInstancesInfo> oldSubgraphInstanceInfo = _subgraphInstancesInfo;

		// update meta data
		_partitionId = partitionId;
		_partitionSliceId = partitionSliceId;
		_subgraphInstancesInfo = subgraphInstancesInfo;
		
		// delete old slices - no exceptions
		try {
			deleteSlice(oldPartitionSliceId);

			for (SubgraphInstancesInfo info : oldSubgraphInstanceInfo.values()) {
				for (UUID sliceId : info.getAllSlices()) {
					deleteSlice(sliceId);
				}
			}
		} catch (Exception e) {}

		return total;
	}

	@Override
	public synchronized void deletePartition() throws IOException {
		if (!_metadataCacheValid) {
			readMetadata();
		}

		deleteSlice(_partitionMetadataSliceId);
		deleteSlice(_partitionSliceId);

		if (_subgraphInstancesInfo != null) {
			for (SubgraphInstancesInfo info : _subgraphInstancesInfo.values()) {
				for (UUID sliceId : info.getAllSlices()) {
					deleteSlice(sliceId);
				}
			}
		}

		_partitionSliceId = null;
		_subgraphInstancesInfo = null;
	}

	protected void deleteSlice(UUID sliceId) {
		if (sliceId != null) {
			try {
				_storageManager.remove(sliceId);
			} catch (IOException e) {
			}
		}
	}
}
