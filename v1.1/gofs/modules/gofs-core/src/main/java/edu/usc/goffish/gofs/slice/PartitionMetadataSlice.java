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

package edu.usc.goffish.gofs.slice;

import java.io.*;
import java.util.*;

import edu.usc.goffish.gofs.*;
import edu.usc.goffish.gofs.util.*;

class PartitionMetadataSlice implements ISlice {

	private static final long serialVersionUID = 1L;

	private final transient UUID _sliceId;

	public final int PartitionId;
	public final UUID PartitionSlice;
	public final Map<Long, SubgraphInstancesInfo> SubgraphInstancesInfo;

	public PartitionMetadataSlice(UUID partitionMetadataSliceId, int partitionId, UUID partitionSlice, Map<Long, SubgraphInstancesInfo> subgraphInstancesInfo) {
		if (partitionMetadataSliceId == null) {
			throw new IllegalArgumentException();
		}
		if (subgraphInstancesInfo == null) {
			throw new IllegalArgumentException();
		}
		
		_sliceId = partitionMetadataSliceId;
		PartitionId = partitionId;
		PartitionSlice = partitionSlice;
		
		// TODO: we need to add kryo serializer for fastutils collections, until then, dump into java collection
		SubgraphInstancesInfo = Collections.unmodifiableMap(new HashMap<>(subgraphInstancesInfo));
	}

	@Override
	public UUID getId() {
		return _sliceId;
	}
	
	static class SubgraphInstancesInfo implements Serializable {

		private static final long serialVersionUID = 1L;

		private final TreeSet<InstanceInfo> _instances;
		private final Map<InstanceTuple, UUID> _instanceSlices;

		public SubgraphInstancesInfo() {
			_instances = new TreeSet<>();
			_instanceSlices = new HashMap<>();
		}

		public SubgraphInstancesInfo(Collection<InstanceInfo> instances, Map<InstanceTuple, UUID> instanceSlice) {
			this();

			_instances.addAll(instances);
			_instanceSlices.putAll(instanceSlice);
		}

		void mapSlice(Collection<ISubgraphInstance> instances, String property, boolean isVertexProperty, UUID sliceId) {
			for (ISubgraphInstance instance : instances) {
				mapSlice(instance.getId(), instance.getTimestampStart(), instance.getTimestampEnd(), property, isVertexProperty, sliceId);
			}
		}

		void mapSlice(ISubgraphInstance instance, String property, boolean isVertexProperty, UUID sliceId) {
			mapSlice(instance.getId(), instance.getTimestampStart(), instance.getTimestampEnd(), property, isVertexProperty, sliceId);
		}

		public void mapSlice(long instanceId, long timestampStart, long timestampEnd, String property, boolean isVertexProperty, UUID sliceId) {
			_instances.add(new InstanceInfo(instanceId, timestampStart, timestampEnd));
			_instanceSlices.put(new InstanceTuple(instanceId, property, isVertexProperty), sliceId);
		}

		public NavigableSet<InstanceInfo> getInstances() {
			return CollectionsUtil.unmodifiableNavigableSet(_instances);
		}

		public NavigableSet<InstanceInfo> getInstances(long timespanStart, boolean startInclusive, long timespanEnd, boolean endInclusive) {
			InstanceInfo lowerBound = new InstanceInfo(Long.MIN_VALUE, timespanStart, timespanStart);
			InstanceInfo upperBound = new InstanceInfo(Long.MAX_VALUE, timespanEnd, timespanEnd);

			return CollectionsUtil.unmodifiableNavigableSet(_instances.subSet(lowerBound, startInclusive, upperBound, endInclusive));
		}

		Map<InstanceTuple, UUID> getSlices() {
			return Collections.unmodifiableMap(_instanceSlices);
		}

		public UUID getSliceFor(long instanceId, String property, boolean isVertexProperty) {
			return _instanceSlices.get(new InstanceTuple(instanceId, property, isVertexProperty));
		}

		public Collection<UUID> getAllSlices() {
			return new HashSet<>(_instanceSlices.values());
		}
	}
	
	static class InstanceInfo implements Comparable<InstanceInfo>, Serializable {

		private static final long serialVersionUID = 1L;
		
		public final long InstanceId;
		public final long TimespanStart;
		public final long TimespanEnd;

		public InstanceInfo(long instanceId, long timespanStart, long timespanEnd) {
			InstanceId = instanceId;
			TimespanStart = timespanStart;
			TimespanEnd = timespanEnd;
		}

		@Override
		public int compareTo(InstanceInfo other) {
			int sn = Long.compare(TimespanStart, other.TimespanStart);
			if (sn == 0) {
				sn = Long.compare(InstanceId, other.InstanceId);
			}
			return sn;
		}
		
		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + (int)(InstanceId ^ (InstanceId >>> 32));
			result = prime * result + (int)(TimespanStart ^ (TimespanStart >>> 32));
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;

			InstanceInfo other = (InstanceInfo)obj;
			return InstanceId == other.InstanceId && TimespanStart == other.TimespanStart;
		}
	}

	static final class InstanceTuple implements Serializable {

		private static final long serialVersionUID = 1L;
		
		public final long InstanceId;
		public final String Property;
		public final boolean IsVertexProperty;

		public InstanceTuple(long instanceId, String property, boolean isVertexProperty) {
			if (property == null) {
				throw new IllegalArgumentException();
			}

			InstanceId = instanceId;
			Property = property;
			IsVertexProperty = isVertexProperty;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + (int)(InstanceId ^ (InstanceId >>> 32));
			result = prime * result + ((Property == null) ? 0 : Property.hashCode());
			result = prime * result + (IsVertexProperty ? 1231 : 1237);
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;

			InstanceTuple other = (InstanceTuple)obj;
			return InstanceId == other.InstanceId && Property.equals(other.Property) && IsVertexProperty == other.IsVertexProperty;
		}
	}
}
