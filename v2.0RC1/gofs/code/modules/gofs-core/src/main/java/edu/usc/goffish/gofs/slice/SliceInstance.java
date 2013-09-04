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

import java.io.*;
import java.util.*;

import edu.usc.goffish.gofs.*;
import edu.usc.goffish.gofs.partition.*;

final class SliceInstance extends BaseInstance {

	private final SliceManager _sliceManager;
	private transient boolean _loaded;
	
	protected SliceInstance(long instanceId, long timestampStart, long timestampEnd, ISubgraph subgraph, PropertySet vertexProperties, PropertySet edgeProperties, SliceManager sliceManager) {
		super(instanceId, timestampStart, timestampEnd, subgraph, vertexProperties, edgeProperties);
		
		if (sliceManager == null) {
			throw new IllegalArgumentException();
		}

		_sliceManager = sliceManager;
		_loaded = false;
	}

	public synchronized void load() {
		if (_loaded) {
			return;
		}

		try {
			// load vertex properties
			for (Property property : getVertexProperties()) {
				_sliceManager.readInstance(getSubgraph().getId(), property.getName(), true, getId(), this);
			}
			
			// load edge properties
			for (Property property : getEdgeProperties()) {
				_sliceManager.readInstance(getSubgraph().getId(), property.getName(), false, getId(), this);
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		
		_loaded = true;
	}
	
	@Override
	public ISubgraphObjectProperties getPropertiesForVertex(long vertexId) {
		if (!_loaded) {
			load();
		}

		return super.getPropertiesForVertex(vertexId);
	}

	@Override
	public Collection<InstancePropertyMap> getPropertiesForVertices() {
		if (!_loaded) {
			load();
		}
		
		return super.getPropertiesForVertices();
	}

	@Override
	public ISubgraphObjectProperties getPropertiesForEdge(long edgeId) {
		if (!_loaded) {
			load();
		}

		return super.getPropertiesForEdge(edgeId);
	}

	@Override
	public Collection<InstancePropertyMap> getPropertiesForEdges() {
		if (!_loaded) {
			load();
		}
		
		return super.getPropertiesForEdges();
	}
	
	// rescope constructor methods to this package
	
	@Override
	protected InstancePropertyMap getPropertiesForVertexForAdd(long vertexId) {
		return super.getPropertiesForVertexForAdd(vertexId);
	}

	@Override
	protected void setPropertiesForVertex(InstancePropertyMap instancePropertyMap) {
		super.setPropertiesForVertex(instancePropertyMap);
	}

	@Override
	protected InstancePropertyMap getPropertiesForEdgeForAdd(long edgeId) {
		return super.getPropertiesForEdgeForAdd(edgeId);
	}

	@Override
	protected void setPropertiesForEdge(InstancePropertyMap instancePropertyMap) {
		super.setPropertiesForEdge(instancePropertyMap);
	}
}
