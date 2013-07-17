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
import edu.usc.goffish.gofs.partition.*;
import edu.usc.goffish.gofs.slice.PartitionMetadataSlice.InstanceInfo;
import edu.usc.goffish.gofs.slice.PartitionMetadataSlice.SubgraphInstancesInfo;
import edu.usc.goffish.gofs.slice.PartitionInstancesSlice.PropertyInstanceTuple;
import edu.usc.goffish.gofs.util.*;

class InstanceIterator extends AbstractWrapperIterator<ISubgraphInstance> {

	private final SliceManager _sliceManager;
	private final SubgraphInstancesInfo _info;
	private final ISubgraph _subgraph;
	
	private final Iterator<InstanceInfo> _itInstances;
	private final PropertySet _vertexProperties;
	private final PropertySet _edgeProperties;

	public InstanceIterator(boolean reverse, SliceManager sliceManager, SubgraphInstancesInfo info, ISubgraph subgraph, long timespanStart, long timespanEnd, PropertySet vertexProperties, PropertySet edgeProperties) {
		if (sliceManager == null) {
			throw new IllegalArgumentException();
		}
		if (subgraph == null) {
			throw new IllegalArgumentException();
		}
		if (timespanStart > timespanEnd) {
			throw new IllegalArgumentException();
		}
		if (vertexProperties == null) {
			throw new IllegalArgumentException();
		}
		if (edgeProperties == null) {
			throw new IllegalArgumentException();
		}
		if (!subgraph.getVertexProperties().containsAll(vertexProperties)) {
			throw new IllegalArgumentException();
		}
		if (!subgraph.getEdgeProperties().containsAll(edgeProperties)) {
			throw new IllegalArgumentException();
		}

		_sliceManager = sliceManager;
		_info = info;
		_subgraph = subgraph;
		
		if (info != null) {
			if (reverse) {
				_itInstances = info.getInstances(timespanStart, true, timespanEnd, false).descendingIterator();
			}else {
				_itInstances = info.getInstances(timespanStart, true, timespanEnd, false).iterator();
			}
		} else {
			_itInstances = null;
		}
		
		_vertexProperties = vertexProperties;
		_edgeProperties = edgeProperties;
	}

	@Override
	protected ISubgraphInstance advanceToNext() {
		if (_itInstances == null || !_itInstances.hasNext()) {
			return null;
		}

		InstanceInfo instanceInfo = _itInstances.next();
		long instanceId = instanceInfo.InstanceId;

		Instance instance = new Instance(instanceId, instanceInfo.TimespanStart, instanceInfo.TimespanEnd, _subgraph.getTemplate(), _vertexProperties, _edgeProperties);

		// load vertex properties
		for (Property property : _vertexProperties) {
			UUID sliceId = _info.getSliceFor(instanceId, property.getName(), true);
			if (sliceId == null) {
				// no slice found for (instanceid/property) tuple
				throw new NoSuchElementException();
			}

			// deserialize the slice
			PartitionInstancesSlice instancePropertySlice;
			try {
				instancePropertySlice = _sliceManager.readInstancesSlice(sliceId);
			} catch (IOException e) {
				throw new IllegalStateException(e);
			}

			if (!instancePropertySlice.InstancesMap.containsKey(instanceId)) {
				throw new IllegalStateException();
			}

			// insert values into instance
			PropertyInstanceTuple tuple = instancePropertySlice.InstancesMap.get(instanceId);
			for (Map.Entry<Long, Object> entry : tuple.InstanceValues.entrySet()) {
				InstancePropertyMap vertexMap = instance.getPropertiesForVertex(entry.getKey());
				vertexMap.setProperty(property.getName(), entry.getValue());
			}
		}

		// load edge properties
		for (Property property : _edgeProperties) {
			UUID sliceId = _info.getSliceFor(instanceId, property.getName(), false);

			// deserialize the slice
			PartitionInstancesSlice instancePropertySlice;
			try {
				instancePropertySlice = _sliceManager.readInstancesSlice(sliceId);
			} catch (IOException e) {
				throw new IllegalStateException(e);
			}

			if (!instancePropertySlice.InstancesMap.containsKey(instanceId)) {
				throw new IllegalStateException();
			}

			// insert values into instance
			PropertyInstanceTuple tuple = instancePropertySlice.InstancesMap.get(instanceId);
			for (Map.Entry<Long, Object> entry : tuple.InstanceValues.entrySet()) {
				InstancePropertyMap edgeMap = instance.getPropertiesForEdge(entry.getKey());
				edgeMap.setProperty(property.getName(), entry.getValue());
			}
		}

		return instance;
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}
}
