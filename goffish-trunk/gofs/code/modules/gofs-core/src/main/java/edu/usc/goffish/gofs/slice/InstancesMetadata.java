/*
s *    Copyright 2013 University of Southern California
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

import java.io.*;
import java.util.*;

import edu.usc.goffish.gofs.util.*;

final class InstancesMetadata implements Serializable {

	private static final long serialVersionUID = 3266581337274180258L;

	private final Long2ObjectMap<TreeSet<InstanceMetadata>> _subgraphInstances; // maps subgraph to list of instances,
																				// used for searching for instances
	private final Map<InstanceTuple, UUID> _instanceSlices; // maps (subgraph, property, instance) to UUID, used to
															// retrieve relevant slices

	@SuppressWarnings("unchecked")
	InstancesMetadata() {
		_subgraphInstances = (Long2ObjectMap<TreeSet<InstanceMetadata>>)Long2ObjectMaps.EMPTY_MAP;
		_instanceSlices = Collections.emptyMap();
	}

	InstancesMetadata(int numSubgraphs) {
		_subgraphInstances = new Long2ObjectOpenHashMap<>(numSubgraphs, 1f);
		_instanceSlices = new HashMap<>();
	}

	void mapInstanceToSlice(InstanceTuple instanceTuple, long timestampStart, long timestampEnd, UUID sliceId) {
		TreeSet<InstanceMetadata> instances = _subgraphInstances.get(instanceTuple.SubgraphId);
		if (instances == null) {
			instances = new TreeSet<>();
			_subgraphInstances.put(instanceTuple.SubgraphId, instances);
		}

		instances.add(new InstanceMetadata(instanceTuple.InstanceId, timestampStart, timestampEnd));
		_instanceSlices.put(instanceTuple, sliceId);
	}

	public NavigableSet<InstanceMetadata> getInstances(long subgraphId) {
		TreeSet<InstanceMetadata> instances = _subgraphInstances.get(subgraphId);
		if (instances == null) {
			return CollectionUtils.emptyNavigableSet();
		} else {
			return CollectionUtils.unmodifiableNavigableSet(instances);
		}
	}

	public NavigableSet<InstanceMetadata> getInstances(long subgraphId, long timespanStart, boolean startInclusive, long timespanEnd, boolean endInclusive) {
		TreeSet<InstanceMetadata> instances = _subgraphInstances.get(subgraphId);
		if (instances == null) {
			return CollectionUtils.emptyNavigableSet();
		} else {
			InstanceMetadata lowerBound = new InstanceMetadata(Long.MIN_VALUE, timespanStart, timespanStart);
			InstanceMetadata upperBound = new InstanceMetadata(Long.MAX_VALUE, timespanEnd, timespanEnd);

			return CollectionUtils.unmodifiableNavigableSet(instances.subSet(lowerBound, startInclusive, upperBound, endInclusive));
		}
	}

	public UUID getSlice(long subgraphId, String property, boolean isVertexProperty, long instanceId) {
		return getSlice(new InstanceTuple(subgraphId, property, isVertexProperty, instanceId));
	}

	protected UUID getSlice(InstanceTuple tuple) {
		return _instanceSlices.get(tuple);
	}

	public Collection<UUID> getAllSlices() {
		return Collections.unmodifiableCollection(_instanceSlices.values());
	}

	public static final class InstanceMetadata implements Comparable<InstanceMetadata>, Serializable {

		private static final long serialVersionUID = 8639000317773772566L;

		public final long InstanceId;
		public final long TimespanStart;
		public final long TimespanEnd;

		public InstanceMetadata(long instanceId, long timespanStart, long timespanEnd) {
			InstanceId = instanceId;
			TimespanStart = timespanStart;
			TimespanEnd = timespanEnd;
		}

		@Override
		public int compareTo(InstanceMetadata other) {
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

			InstanceMetadata other = (InstanceMetadata)obj;
			return InstanceId == other.InstanceId && TimespanStart == other.TimespanStart;
		}
	}

	static final class InstanceTuple implements Serializable {

		private static final long serialVersionUID = 4423757466073281991L;

		public final long SubgraphId;
		public final String Property;
		public final boolean IsVertexProperty;
		public final long InstanceId;

		public InstanceTuple(long subgraphId, String property, boolean isVertexProperty, long instanceId) {
			if (property == null) {
				throw new IllegalArgumentException();
			}

			SubgraphId = subgraphId;
			Property = property;
			IsVertexProperty = isVertexProperty;
			InstanceId = instanceId;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + (int)(SubgraphId ^ (SubgraphId >>> 32));
			result = prime * result + ((Property == null) ? 0 : Property.hashCode());
			result = prime * result + (IsVertexProperty ? 1231 : 1237);
			result = prime * result + (int)(InstanceId ^ (InstanceId >>> 32));
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
			return SubgraphId == other.SubgraphId && Property.equals(other.Property) && IsVertexProperty == other.IsVertexProperty && InstanceId == other.InstanceId;
		}

		@Override
		public String toString() {
			return getClass().getSimpleName() + "@[Subgraph=" + SubgraphId + ", Property=" + Property + ", IsVertexProperty=" + IsVertexProperty + ", Instance=" + InstanceId + "]";
		}
	}
}
