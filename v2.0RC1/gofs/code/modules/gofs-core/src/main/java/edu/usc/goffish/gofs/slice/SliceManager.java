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

package edu.usc.goffish.gofs.slice;

import it.unimi.dsi.fastutil.longs.*;
import it.unimi.dsi.fastutil.objects.*;

import java.io.*;
import java.lang.ref.*;
import java.util.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.*;

import com.esotericsoftware.kryo.*;

import edu.usc.goffish.gofs.*;
import edu.usc.goffish.gofs.partition.*;
import edu.usc.goffish.gofs.slice.InstancesMetadata.InstanceMetadata;
import edu.usc.goffish.gofs.slice.InstancesMetadata.InstanceTuple;
import edu.usc.goffish.gofs.util.*;

public class SliceManager implements ISliceManager {

	private static final int MAX_CACHE_CAPACITY = 100;
	private static final int SLICE_VERSION = 5;

	// all important meta data slice
	private final UUID _partitionMetadataSliceId;

	private final ReentrantReadWriteLock _managerLock;

	// protected by simple reader/writer lock with synchronized/volatile
	private volatile boolean _metadataCacheValid;
	private volatile int _modificationCount;

	// cached meta data information - protected by _managerLock
	private int _partitionId;
	private UUID _partitionSliceId;
	private InstancesMetadata _instancesMetadata;

	private final ISliceSerializer _sliceSerializer;
	private final IStorageManager _storageManager;

	private final SliceMRUCache _cache;

	private SliceManager(UUID partitionMetadataSliceId, ISliceSerializer sliceSerializer, IStorageManager storageManager) {
		if (partitionMetadataSliceId == null) {
			throw new IllegalArgumentException();
		}
		if (sliceSerializer == null) {
			throw new IllegalArgumentException();
		}
		if (storageManager == null) {
			throw new IllegalArgumentException();
		}

		_managerLock = new ReentrantReadWriteLock();
		_modificationCount = 0;

		_partitionMetadataSliceId = partitionMetadataSliceId;
		_sliceSerializer = sliceSerializer;
		_storageManager = storageManager;
		_metadataCacheValid = false;

		_partitionId = Partition.INVALID_PARTITION;
		_partitionSliceId = null;
		_instancesMetadata = new InstancesMetadata();

		_cache = new SliceMRUCache(MAX_CACHE_CAPACITY);
	}

	public static ISliceManager create(ISliceSerializer sliceSerializer, IStorageManager storageManager) {
		return create(UUID.randomUUID(), sliceSerializer, storageManager);
	}

	public static ISliceManager create(UUID partitionMetadataSliceId, ISliceSerializer sliceSerializer, IStorageManager storageManager) {
		return new SliceManager(partitionMetadataSliceId, sliceSerializer, storageManager);
	}

	@Override
	public UUID getPartitionUUID() {
		return _partitionMetadataSliceId;
	}

	@Override
	public int getPartitionId() {
		_managerLock.readLock().lock();
		try {
			return _partitionId;
		} finally {
			_managerLock.readLock().unlock();
		}
	}

	public UUID getPartitionTemplateUUID() {
		_managerLock.readLock().lock();
		try {
			return _partitionSliceId;
		} finally {
			_managerLock.readLock().unlock();
		}
	}

	@Override
	public synchronized void invalidate() {
		_metadataCacheValid = false;
		_modificationCount++;
	}

	int getModificationCount() {
		return _modificationCount;
	}

	public int getCacheQueries() {
		return _cache.getApproxCacheQueries();
	}

	public int getCacheHits() {
		return _cache.getApproxCacheHits();
	}

	private synchronized void readMetadata() throws IOException {
		if (_metadataCacheValid) {
			return;
		}

		_managerLock.writeLock().lock();
		try {
			if (_storageManager.hasReadStream(_partitionMetadataSliceId)) {
				PartitionMetadataSlice metadata;
				try (InputStream in = _sliceSerializer.prepareStream(_storageManager.getReadStream(_partitionMetadataSliceId))) {
					VersioningSlice versioningSlice = _sliceSerializer.deserialize(in, VersioningSlice.class);
					if (versioningSlice.Version != SLICE_VERSION) {
						throw new IOException("partition with metadata slice uuid " + _partitionMetadataSliceId + " is version " + versioningSlice.Version + " and cannot be read as version " + SLICE_VERSION);
					}

					metadata = _sliceSerializer.deserialize(in, PartitionMetadataSlice.class);
				} catch (KryoException e) {
					throw new RuntimeException("exception while reading metadata slice " + _partitionMetadataSliceId, e);
				}

				_partitionId = metadata.getPartitionId();
				_partitionSliceId = metadata.getPartitionSlice();
				_instancesMetadata = metadata.getInstancesMetadata();
			} else {
				_partitionId = Partition.INVALID_PARTITION;
				_partitionSliceId = null;
				_instancesMetadata = new InstancesMetadata();
			}

			_cache.clearSlices();
		} finally {
			_managerLock.writeLock().unlock();
		}

		_modificationCount++;
		_metadataCacheValid = true;
	}

	private synchronized long writeNewMetadata(int partitionId, UUID partitionTemplateSliceId, InstancesMetadata instancesMetadata) throws IOException {
		if (!_metadataCacheValid) {
			// may not write metadata when cache is stale
			throw new IllegalStateException();
		}

		long total = 0;

		_managerLock.readLock().lock();
		try {
			// we'd like this to fail without corrupting any data, but that's impossible unless we change metadata slice
			// ids every time...
			VersioningSlice versioningSlice = new VersioningSlice(SLICE_VERSION);
			PartitionMetadataSlice metadataSlice = new PartitionMetadataSlice(_partitionMetadataSliceId, partitionId, partitionTemplateSliceId, instancesMetadata);
			try (OutputStream out = _sliceSerializer.prepareStream(_storageManager.getWriteStream(metadataSlice.getId()))) {
				total += _sliceSerializer.serialize(versioningSlice, out);
				total += _sliceSerializer.serialize(metadataSlice, out);
				_modificationCount++;
			}
		} finally {
			_managerLock.readLock().unlock();
		}

		return total;
	}

	@Override
	public IPartition readPartition() throws IOException {
		if (!_metadataCacheValid) {
			readMetadata();
		}

		_managerLock.readLock().lock();
		try {
			if (_partitionId == Partition.INVALID_PARTITION) {
				throw new FileNotFoundException("Slice " + _partitionMetadataSliceId + " not found");
			}
			assert (_partitionSliceId != null);

			// deserialize the slice
			PartitionSlice partitionSlice;
			try (InputStream in = _sliceSerializer.prepareStream(_storageManager.getReadStream(_partitionSliceId))) {
				partitionSlice = _sliceSerializer.deserialize(in, PartitionSlice.class);
			} catch (KryoException e) {
				throw new RuntimeException("exception while reading partition slice " + _partitionSliceId, e);
			}

			return new Partition(_partitionId, partitionSlice.isDirected(), partitionSlice.buildSubgraphs(this), partitionSlice.getVertexProperties(), partitionSlice.getEdgeProperties());
		} finally {
			_managerLock.readLock().unlock();
		}
	}

	Iterable<ISubgraphInstance> getInstances(final ISubgraph subgraph, long startTime, long endTime, final PropertySet vertexProperties, final PropertySet edgeProperties, boolean reverse) throws IOException {
		if (subgraph == null) {
			throw new IllegalArgumentException();
		}
		if (startTime > endTime) {
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

		_managerLock.readLock().lock();
		try {
			if (_partitionId == Partition.INVALID_PARTITION) {
				throw new FileNotFoundException("Slice " + _partitionMetadataSliceId + " not found");
			}
			assert (_instancesMetadata != null);

			// retrieve list of instances
			final Iterable<InstanceMetadata> instances;
			if (!reverse) {
				instances = _instancesMetadata.getInstances(subgraph.getId(), startTime, true, endTime, false);
			} else {
				instances = _instancesMetadata.getInstances(subgraph.getId(), startTime, true, endTime, false).descendingSet();
			}

			return new Iterable<ISubgraphInstance>() {
				@Override
				public Iterator<ISubgraphInstance> iterator() {
					return new InstanceIterator(subgraph, instances.iterator(), vertexProperties, edgeProperties);
				}
			};
		} finally {
			_managerLock.readLock().unlock();
		}
	}

	public void readInstance(long subgraphId, String propertyName, boolean isVertexProperty, long instanceId, SliceInstance instance) throws IOException {
		readInstance(new InstanceTuple(subgraphId, propertyName, isVertexProperty, instanceId), instance);
	}

	void readInstance(InstanceTuple tuple, SliceInstance instance) throws IOException {
		PartitionInstancesSlice partitionInstancesSlice;

		_managerLock.readLock().lock();
		try {
			UUID sliceId = _instancesMetadata.getSlice(tuple);
			if (sliceId == null) {
				// no slice found
				return;
			}

			partitionInstancesSlice = (PartitionInstancesSlice)_cache.getSlice(sliceId);
			if (partitionInstancesSlice == null) {
				try (InputStream in = _sliceSerializer.prepareStream(_storageManager.getReadStream(sliceId))) {
					partitionInstancesSlice = _sliceSerializer.deserialize(in, PartitionInstancesSlice.class);
				} catch (KryoException e) {
					throw new RuntimeException("exception while reading instance slice " + sliceId, e);
				}

				_cache.putSlice(sliceId, partitionInstancesSlice);
			}

			boolean inserted = partitionInstancesSlice.fillInstance(tuple, instance);
			if (!inserted) {
				// no relevant data found in slice
				throw new IllegalStateException("no relevant data found in slice " + sliceId + "(query: " + tuple + ")");
			}
		} finally {
			_managerLock.readLock().unlock();
		}
	}

	public String printInstance(UUID sliceId) throws IOException {
		PartitionInstancesSlice partitionInstancesSlice;
		try (InputStream in = _sliceSerializer.prepareStream(_storageManager.getReadStream(sliceId))) {
			partitionInstancesSlice = _sliceSerializer.deserialize(in, PartitionInstancesSlice.class);
		} catch (KryoException e) {
			throw new IOException("exception while reading instance slice " + sliceId, e);
		}

		return partitionInstancesSlice.toString();
	}

	@Override
	public synchronized long writePartitionTemplate(IPartition partition) throws IOException {
		if (partition == null) {
			throw new IllegalArgumentException();
		}

		if (!_metadataCacheValid) {
			readMetadata();
		}

		long total = 0;

		// hold onto old metadata for deletion
		UUID oldPartitionSliceId = _partitionSliceId;

		_managerLock.writeLock().lock();
		try {
			if (_partitionId != Partition.INVALID_PARTITION && partition.getId() != _partitionId) {
				throw new IllegalArgumentException();
			}

			// the ordering of every operation in this function is important to
			// guarantee consistency, be careful changing it

			// new meta data
			int partitionId = Partition.INVALID_PARTITION;
			UUID partitionSliceId = null;

			try {
				PartitionSlice partitionSlice = new PartitionSlice(partition);

				// update new meta data
				partitionId = partition.getId();
				partitionSliceId = partitionSlice.getId();

				// serialize partition
				try (OutputStream out = _sliceSerializer.prepareStream(_storageManager.getWriteStream(partitionSlice.getId()))) {
					total += _sliceSerializer.serialize(partitionSlice, out);
				}

				// serialize new meta data - this MUST be the last item, if this does not fail, there is no going back
				total += writeNewMetadata(partitionId, partitionSliceId, _instancesMetadata);
			} catch (Exception e) {
				// clean up
				if (partitionSliceId != null) {
					deleteSlice(partitionSliceId);
				}

				throw e;
			}

			// update meta data
			_partitionId = partitionId;
			_partitionSliceId = partitionSliceId;
			_cache.clearSlices();
		} finally {
			_managerLock.writeLock().unlock();
		}

		// delete old slices
		try {
			deleteSlice(oldPartitionSliceId);
		} catch (Exception e) {
		}

		return total;
	}

	@Override
	public synchronized long writePartition(ISerializablePartition partition, int instancesGroupingSize, int numSubgraphBins) throws IOException {
		if (partition == null) {
			throw new IllegalArgumentException();
		}
		if (instancesGroupingSize < 1) {
			throw new IllegalArgumentException();
		}
		if (numSubgraphBins < 1 || numSubgraphBins > partition.size()) {
			throw new IllegalArgumentException();
		}

		if (!_metadataCacheValid) {
			readMetadata();
		}

		long total = 0;

		// hold onto old metadata for deletion
		UUID oldPartitionSliceId = _partitionSliceId;
		InstancesMetadata oldInstancesMetadata = _instancesMetadata;

		// calculate subgraph bins
		Iterable<? extends Collection<ISubgraph>> subgraphBins = calculateBins(partition, numSubgraphBins);

		_managerLock.writeLock().lock();
		try {
			if (_partitionId != Partition.INVALID_PARTITION && partition.getId() != _partitionId) {
				throw new IllegalArgumentException();
			}

			// the ordering of every operation in this function is important to
			// guarantee consistency, be careful changing it

			// new meta data
			int partitionId = Partition.INVALID_PARTITION;
			UUID partitionSliceId = null;
			InstancesMetadata instancesMetadata = new InstancesMetadata(partition.size());

			try {
				// scoped for gc
				{
					PartitionSlice partitionSlice = new PartitionSlice(partition, subgraphBins);

					// update new meta data
					partitionId = partition.getId();
					partitionSliceId = partitionSlice.getId();

					// serialize partition
					try (OutputStream out = _sliceSerializer.prepareStream(_storageManager.getWriteStream(partitionSlice.getId()))) {
						total += _sliceSerializer.serialize(partitionSlice, out);
					}
				}

				// scoped for gc
				{
					// loop instances
					Iterator<Long2ObjectMap<? extends ISubgraphInstance>> itInstances = partition.getSubgraphsInstances().iterator();
					while (itInstances.hasNext()) {

						// accumulate instances
						List<Long2ObjectMap<? extends ISubgraphInstance>> groupedInstances = new LinkedList<>();
						for (int i = 0; i < instancesGroupingSize && itInstances.hasNext(); i++) {
							groupedInstances.add(itInstances.next());
						}

						// loop subgraphs
						Iterator<? extends Collection<ISubgraph>> itSubgraphs = subgraphBins.iterator();
						while (itSubgraphs.hasNext()) {

							// accumulate subgraphs
							Collection<ISubgraph> groupedSubgraphs = itSubgraphs.next();

							// for every vertex property...
							for (Property property : partition.getVertexProperties()) {

								PartitionInstancesSlice instancesSlice = new PartitionInstancesSlice();

								// loop over every combination
								for (Long2ObjectMap<? extends ISubgraphInstance> subgraphInstances : groupedInstances) {
									for (ISubgraph subgraph : groupedSubgraphs) {
										if (!subgraphInstances.containsKey(subgraph.getId())) {
											continue;
										}

										ISubgraphInstance instance = subgraphInstances.get(subgraph.getId());

										// skip empty instance
										if (!instance.hasProperties()) {
											continue;
										}

										// add to slice
										InstanceTuple instanceTuple = new InstanceTuple(subgraph.getId(), property.getName(), true, instance.getId());
										instancesSlice.addInstance(instanceTuple, instance);

										// update metadata
										instancesMetadata.mapInstanceToSlice(instanceTuple, instance.getTimestampStart(), instance.getTimestampEnd(), instancesSlice.getId());
									}
								}

								// skip serialization if slice is empty
								if (instancesSlice.isEmpty()) {
									System.out.println("serialization skipped");
									continue;
								}

								// serialize instances
								try (OutputStream out = _sliceSerializer.prepareStream(_storageManager.getWriteStream(instancesSlice.getId()))) {
									total += _sliceSerializer.serialize(instancesSlice, out);
								}
							}

							// for every edge property...
							for (Property property : partition.getEdgeProperties()) {

								PartitionInstancesSlice instancesSlice = new PartitionInstancesSlice();

								// loop over every single combination
								for (Long2ObjectMap<? extends ISubgraphInstance> subgraphInstances : groupedInstances) {
									for (ISubgraph subgraph : groupedSubgraphs) {
										if (!subgraphInstances.containsKey(subgraph.getId())) {
											continue;
										}

										ISubgraphInstance instance = subgraphInstances.get(subgraph.getId());

										// skip empty instance
										if (!instance.hasProperties()) {
											continue;
										}

										// add to slice
										InstanceTuple instanceTuple = new InstanceTuple(subgraph.getId(), property.getName(), false, instance.getId());
										instancesSlice.addInstance(instanceTuple, instance);

										// update metadata
										instancesMetadata.mapInstanceToSlice(instanceTuple, instance.getTimestampStart(), instance.getTimestampEnd(), instancesSlice.getId());
									}
								}

								// skip serialization if slice is empty
								if (instancesSlice.isEmpty()) {
									System.out.println("serialization skipped");
									continue;
								}

								// serialize instances
								try (OutputStream out = _sliceSerializer.prepareStream(_storageManager.getWriteStream(instancesSlice.getId()))) {
									total += _sliceSerializer.serialize(instancesSlice, out);
								}
							}
						}
					}
				}

				// serialize new meta data - this MUST be the last item, if this does not fail, there is no going back
				total += writeNewMetadata(partitionId, partitionSliceId, instancesMetadata);
			} catch (Exception e) {
				// clean up
				if (partitionSliceId != null) {
					deleteSlice(partitionSliceId);
				}

				for (UUID sliceId : instancesMetadata.getAllSlices()) {
					deleteSlice(sliceId);
				}

				throw e;
			}

			// update meta data
			_partitionId = partitionId;
			_partitionSliceId = partitionSliceId;
			_instancesMetadata = instancesMetadata;
			_cache.clearSlices();
		} finally {
			_managerLock.writeLock().unlock();
		}

		// delete old slices
		try {
			deleteSlice(oldPartitionSliceId);
			for (UUID sliceId : oldInstancesMetadata.getAllSlices()) {
				deleteSlice(sliceId);
			}
		} catch (Exception e) {
		}

		return total;
	}

	@Override
	public synchronized void deletePartition() throws IOException {
		if (!_metadataCacheValid) {
			readMetadata();
		}

		UUID oldPartitionSliceId = _partitionSliceId;
		InstancesMetadata oldInstancesMetadata = _instancesMetadata;

		_managerLock.writeLock().lock();
		try {
			// delete meta data but save partition id
			int partitionId = _partitionId;
			deleteSlice(_partitionMetadataSliceId);
			readMetadata();
			_partitionId = partitionId;
		} finally {
			_managerLock.writeLock().unlock();
		}

		// cleanup
		deleteSlice(oldPartitionSliceId);
		for (UUID sliceId : oldInstancesMetadata.getAllSlices()) {
			deleteSlice(sliceId);
		}
	}

	protected void deleteSlice(UUID sliceId) {
		if (sliceId != null) {
			try {
				_storageManager.remove(sliceId);
			} catch (IOException e) {
			}
		}
	}

	private static Iterable<? extends Collection<ISubgraph>> calculateBins(IPartition partition, int numSubgraphBins) {
		if (partition.size() == 1) {
			return Collections.singletonList(Collections.singletonList(partition.iterator().next()));
		} else if (numSubgraphBins == 1) {
			return Collections.singletonList(new ArrayList<>(partition));
		} else if (numSubgraphBins >= partition.size()) {
			ArrayList<List<ISubgraph>> bins = new ArrayList<>(partition.size());
			for (ISubgraph subgraph : partition) {
				bins.add(Collections.singletonList(subgraph));
			}
			return bins;
		}

		// sort subgraphs for bin packing
		ISubgraph[] sortedSubgraphs = partition.toArray(new ISubgraph[partition.size()]);
		Arrays.sort(sortedSubgraphs, Collections.reverseOrder(new BaseSubgraph.SubgraphVertexCountComparator()));

		// initialize bins
		ArrayList<SubgraphBinPackingList> bins = new ArrayList<>(numSubgraphBins);
		ObjectHeapPriorityQueue<SubgraphBinPackingList> sortedBins = new ObjectHeapPriorityQueue<SubgraphBinPackingList>(numSubgraphBins);
		for (int i = 0; i < numSubgraphBins; i++) {
			SubgraphBinPackingList l = new SubgraphBinPackingList();
			bins.add(l);
			sortedBins.enqueue(l);
		}

		// simple bin packing algorithm iterates over subgraphs from largest to smallest and puts each in the smallest
		// bin. runtime is O(numSubgraphs*log(numBins)), which when numBins is a small constant devolves to
		// approximately O(N). performance improvements possible by using Fibonacci heap, Hochbaum and Shmoys PTAS, or
		// DP solution
		for (ISubgraph subgraph : sortedSubgraphs) {
			sortedBins.first().add(subgraph);
			sortedBins.changed();
		}

		return bins;
	}

	private static class SubgraphBinPackingList extends LinkedList<ISubgraph> implements Comparable<SubgraphBinPackingList> {

		private static final long serialVersionUID = -6049183624749206530L;

		private int _total = 0;

		@Override
		public boolean add(ISubgraph subgraph) {
			if (super.add(subgraph)) {
				_total += subgraph.numVertices();
				return true;
			}

			return false;
		}

		@Override
		public int compareTo(SubgraphBinPackingList o) {
			return _total - o._total;
		}
	}

	private class InstanceIterator extends AbstractWrapperIterator<ISubgraphInstance> {

		private final ISubgraph _subgraph;
		private final int _modificationCountAtInstantiation;

		private final Iterator<InstanceMetadata> _itInstances;
		private final PropertySet _vertexProperties;
		private final PropertySet _edgeProperties;

		public InstanceIterator(ISubgraph subgraph, Iterator<InstanceMetadata> itInstances, PropertySet vertexProperties, PropertySet edgeProperties) {
			_subgraph = subgraph;
			_modificationCountAtInstantiation = getModificationCount();

			_itInstances = itInstances;
			_vertexProperties = vertexProperties;
			_edgeProperties = edgeProperties;
		}

		@Override
		protected ISubgraphInstance advanceToNext() {
			if (_modificationCountAtInstantiation != getModificationCount()) {
				throw new ConcurrentModificationException();
			}

			if (!_itInstances.hasNext()) {
				return null;
			}

			InstanceMetadata instanceInfo = _itInstances.next();
			return new SliceInstance(instanceInfo.InstanceId, instanceInfo.TimespanStart, instanceInfo.TimespanEnd, _subgraph, _vertexProperties, _edgeProperties, SliceManager.this);
		}
	}

	private static class SliceMRUCache extends LinkedHashMap<UUID, CacheSoftReference> {

		private static final long serialVersionUID = 1L;

		private final int _capacity;
		private final ReferenceQueue<ISlice> _referenceQueue;

		private final ReentrantReadWriteLock _cacheLock;

		private final AtomicInteger _cacheQueries;
		private final AtomicInteger _cacheHits;

		public SliceMRUCache(int capacity) {
			super(capacity, 1f, true);
			_capacity = capacity;
			_referenceQueue = new ReferenceQueue<>();
			_cacheLock = new ReentrantReadWriteLock();

			_cacheQueries = new AtomicInteger(0);
			_cacheHits = new AtomicInteger(0);
		}

		public int getApproxCacheQueries() {
			return _cacheQueries.get();
		}

		public int getApproxCacheHits() {
			return _cacheHits.get();
		}

		public void putSlice(UUID id, ISlice slice) {
			if (slice == null) {
				throw new IllegalArgumentException();
			}

			_cacheLock.writeLock().lock();
			try {
				CacheSoftReference r;
				while ((r = (CacheSoftReference)_referenceQueue.poll()) != null) {
					remove(r._uuid);
				}

				r = new CacheSoftReference(slice);
				put(id, r);
			} finally {
				_cacheLock.writeLock().unlock();
			}
		}

		public ISlice getSlice(UUID id) {
			_cacheQueries.incrementAndGet();

			_cacheLock.readLock().lock();
			try {
				CacheSoftReference r = get(id);
				if (r == null) {
					return null;
				}

				_cacheHits.incrementAndGet();
				return r.get();
			} finally {
				_cacheLock.readLock().unlock();
			}
		}

		public void clearSlices() {
			_cacheLock.writeLock().lock();
			try {
				clear();
			} finally {
				_cacheLock.writeLock().unlock();
			}
		}

		@Override
		protected boolean removeEldestEntry(Map.Entry<UUID, CacheSoftReference> eldest) {
			return size() > _capacity;
		}
	}

	private static class CacheSoftReference extends SoftReference<ISlice> {

		private final UUID _uuid;

		public CacheSoftReference(ISlice referent) {
			super(referent);
			_uuid = referent.getId();
		}
	}
}
