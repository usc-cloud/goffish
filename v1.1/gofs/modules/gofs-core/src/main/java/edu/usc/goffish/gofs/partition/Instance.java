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
package edu.usc.goffish.gofs.partition;

import java.util.*;

import edu.usc.goffish.gofs.*;

public class Instance implements ISubgraphInstance {

	private final long _instanceId;
	private final long _timestampStart;
	private final long _timestampEnd;

	private final ISubgraphTemplate _template;

	private final PropertySet _vertexProperties;
	private final PropertySet _edgeProperties;

	private final Map<Long, InstancePropertyMap> _vertexInstances;
	private final Map<Long, InstancePropertyMap> _edgeInstances;

	public Instance(long instanceId, long timestampStart, long timestampEnd, ISubgraphTemplate template, PropertySet vertexProperties, PropertySet edgeProperties) {
		_instanceId = instanceId;
		_timestampStart = timestampStart;
		_timestampEnd = timestampEnd;

		_template = template;

		_vertexProperties = vertexProperties;
		_edgeProperties = edgeProperties;

		_vertexInstances = new HashMap<>();
		_edgeInstances = new HashMap<>();
	}

	@Override
	public long getId() {
		return _instanceId;
	}

	@Override
	public long getTimestampStart() {
		return _timestampStart;
	}

	@Override
	public long getTimestampEnd() {
		return _timestampEnd;
	}

	@Override
	public ISubgraphTemplate getTemplate() {
		return _template;
	}

	@Override
	public PropertySet getVertexProperties() {
		return _vertexProperties;
	}

	@Override
	public PropertySet getEdgeProperties() {
		return _edgeProperties;
	}

	@Override
	public InstancePropertyMap getPropertiesForVertex(long vertexId) {
		assert(_template.containsVertex(vertexId));
		
		if (!_vertexInstances.containsKey(vertexId)) {
			_vertexInstances.put(vertexId, new InstancePropertyMap(vertexId, getVertexProperties()));
		}

		return _vertexInstances.get(vertexId);
	}

	@Override
	public Collection<InstancePropertyMap> getPropertiesForVertices() {
		return Collections.unmodifiableCollection(_vertexInstances.values());
	}

	@Override
	public InstancePropertyMap getPropertiesForEdge(long edgeId) {
		if (!_edgeInstances.containsKey(edgeId)) {
			_edgeInstances.put(edgeId, new InstancePropertyMap(edgeId, getEdgeProperties()));
		}
		
		return _edgeInstances.get(edgeId);
	}

	@Override
	public Collection<InstancePropertyMap> getPropertiesForEdges() {
		return Collections.unmodifiableCollection(_edgeInstances.values());
	}

	public void addPropertiesForVertex(InstancePropertyMap instancePropertyMap) {
		if (instancePropertyMap == null) {
			throw new IllegalArgumentException();
		}
		if (_vertexInstances.containsKey(instancePropertyMap.getId())) {
			// property already set for this vertex
			throw new IllegalArgumentException();
		}
		
		assert(_template.containsVertex(instancePropertyMap.getId()));

		_vertexInstances.put(instancePropertyMap.getId(), instancePropertyMap);
	}

	public void addPropertiesForEdge(InstancePropertyMap instancePropertyMap) {
		if (instancePropertyMap == null) {
			throw new IllegalArgumentException();
		}
		if (_edgeInstances.containsKey(instancePropertyMap.getId())) {
			// property already set for this edge
			throw new IllegalArgumentException();
		}

		_edgeInstances.put(instancePropertyMap.getId(), instancePropertyMap);
	}
	
	@Override
	public String toString() {
		return "Instance@" + _instanceId + "[start=" + _timestampStart + ",end=" + _timestampEnd + ",vertexProperties=" + _vertexProperties + ",edgeProperties=" + _edgeProperties + "]";
	}
}
