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

package edu.usc.goffish.gofs.partition;

import it.unimi.dsi.fastutil.longs.*;

import java.util.*;

import edu.usc.goffish.gofs.*;

public class BaseInstance implements ISubgraphInstance {

	private final long _instanceId;
	private final long _timestampStart;
	private final long _timestampEnd;

	private final ISubgraph _subgraph;

	private final PropertySet _vertexProperties;
	private final PropertySet _edgeProperties;

	private final Long2ObjectMap<InstancePropertyMap> _vertexInstances;
	private final Long2ObjectMap<InstancePropertyMap> _edgeInstances;

	protected BaseInstance(long instanceId, long timestampStart, long timestampEnd, ISubgraph subgraph, PropertySet vertexProperties, PropertySet edgeProperties) {
		if (timestampStart > timestampEnd) {
			throw new IllegalArgumentException();
		}
		if (subgraph == null) {
			throw new IllegalArgumentException();
		}
		if (vertexProperties == null) {
			throw new IllegalArgumentException();
		}
		if (edgeProperties == null) {
			throw new IllegalArgumentException();
		}
		
		_instanceId = instanceId;
		_timestampStart = timestampStart;
		_timestampEnd = timestampEnd;

		_subgraph = subgraph;

		_vertexProperties = vertexProperties;
		_edgeProperties = edgeProperties;
		
		_vertexInstances = new Long2ObjectOpenHashMap<>();
		_edgeInstances = new Long2ObjectOpenHashMap<>();
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
	public ISubgraphTemplate<? extends ITemplateVertex, ? extends ITemplateEdge> getTemplate() {
		return _subgraph.getTemplate();
	}

	public ISubgraph getSubgraph() {
		return _subgraph;
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
	public boolean hasProperties() {
		return !_vertexInstances.isEmpty() || !_edgeInstances.isEmpty();
	}
	
	@Override
	public ISubgraphObjectProperties getPropertiesForVertex(long vertexId) {
		assert(getTemplate().containsVertex(vertexId));
		
		ISubgraphObjectProperties properties = _vertexInstances.get(vertexId);
		if (properties == null) {
			properties = new InstancePropertyMap(vertexId, getVertexProperties());
		}
		
		return properties;
	}

	@Override
	public Collection<InstancePropertyMap> getPropertiesForVertices() {
		return Collections.unmodifiableCollection(_vertexInstances.values());
	}

	@Override
	public ISubgraphObjectProperties getPropertiesForEdge(long edgeId) {
		ISubgraphObjectProperties properties = _edgeInstances.get(edgeId);
		if (properties == null) {
			properties = new InstancePropertyMap(edgeId, getEdgeProperties());
		}
		
		return properties;
	}

	@Override
	public Collection<InstancePropertyMap> getPropertiesForEdges() {
		return Collections.unmodifiableCollection(_edgeInstances.values());
	}
	
	protected InstancePropertyMap getPropertiesForVertexForAdd(long vertexId) {
		assert(getTemplate().containsVertex(vertexId));
		
		InstancePropertyMap propertyMap = _vertexInstances.get(vertexId);
		if (propertyMap == null) {
			propertyMap = new InstancePropertyMap(vertexId, getVertexProperties());
			_vertexInstances.put(vertexId, propertyMap);
		}

		return propertyMap;
	}
	
	protected void setPropertiesForVertex(InstancePropertyMap instancePropertyMap) {
		if (instancePropertyMap == null) {
			throw new IllegalArgumentException();
		}
		if (_vertexInstances.containsKey(instancePropertyMap.getId())) {
			// property map already set for this vertex
			throw new IllegalArgumentException();
		}
		
		assert(getTemplate().containsVertex(instancePropertyMap.getId()));
		_vertexInstances.put(instancePropertyMap.getId(), instancePropertyMap);
	}
	
	protected InstancePropertyMap getPropertiesForEdgeForAdd(long edgeId) {
		InstancePropertyMap propertyMap = _edgeInstances.get(edgeId);
		if (propertyMap == null) {
			propertyMap = new InstancePropertyMap(edgeId, getEdgeProperties());
			_edgeInstances.put(edgeId, propertyMap);
		}

		return propertyMap;
	}
	
	protected void setPropertiesForEdge(InstancePropertyMap instancePropertyMap) {
		if (instancePropertyMap == null) {
			throw new IllegalArgumentException();
		}
		if (_edgeInstances.containsKey(instancePropertyMap.getId())) {
			// property map already set for this edge
			throw new IllegalArgumentException();
		}

		_edgeInstances.put(instancePropertyMap.getId(), instancePropertyMap);
	}
	
	@Override
	public String toString() {
		return "Instance@" + _instanceId + "[start=" + _timestampStart + ", end=" + _timestampEnd + ", vertexProperties=" + _vertexProperties + ", edgeProperties=" + _edgeProperties + "]";
	}
}
