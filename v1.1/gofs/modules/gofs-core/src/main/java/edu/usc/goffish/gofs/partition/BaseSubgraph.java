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

import it.unimi.dsi.fastutil.longs.*;

import java.io.*;
import java.util.*;

import edu.usc.goffish.gofs.*;

public abstract class BaseSubgraph implements ISubgraph {

	private final long _id;
	private final ISubgraphTemplate _template;

	private final PropertySet _vertexProperties;
	private final PropertySet _edgeProperties;

	private final Long2IntMap _remoteVertices;
	
	public BaseSubgraph(long id, ISubgraphTemplate template, PropertySet vertexProperties, PropertySet edgeProperties, Map<Long, Integer> remoteVertices) {
		if (template == null) {
			throw new IllegalArgumentException();
		}
		if (vertexProperties == null) {
			throw new IllegalArgumentException();
		}
		if (edgeProperties == null) {
			throw new IllegalArgumentException();
		}
		if (remoteVertices == null) {
			throw new IllegalArgumentException();
		}

		_id = id;
		_template = template;

		_vertexProperties = vertexProperties;
		_edgeProperties = edgeProperties;

		// TODO: find a way to avoid defensive copy
		_remoteVertices = Long2IntMaps.unmodifiable(new Long2IntOpenHashMap(remoteVertices));
	}

	@Override
	public long getId() {
		return _id;
	}

	@Override
	public boolean isDirected() {
		return _template.isDirected();
	}

	@Override
	public Iterable<? extends TemplateVertex> vertices() {
		return _template.vertices();
	}

	@Override
	public Iterable<? extends TemplateEdge> edges() {
		return _template.edges();
	}

	@Override
	public int numVertices() {
		return _template.numVertices();
	}

	@Override
	public long numEdges() {
		return _template.numEdges();
	}
	
	@Override
	public TemplateVertex getVertex(long vertexId) {
		return _template.getVertex(vertexId);
	}

	@Override
	public boolean containsVertex(long vertexId) {
		return _template.containsVertex(vertexId);
	}
	
	@Override
	public TemplateEdge getEdge(long edgeId) {
		return _template.getEdge(edgeId);
	}

	@Override
	public boolean containsEdge(long edgeId) {
		return _template.containsEdge(edgeId);
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
	public ISubgraphTemplate getTemplate() {
		return _template;
	}

	public Iterable<? extends ISubgraphInstance> getInstances() throws IOException {
		return getInstances(Long.MIN_VALUE, Long.MAX_VALUE, getVertexProperties(), getEdgeProperties(), false);
	}

	public Iterable<? extends ISubgraphInstance> getInstances(long startTime, long endTime, PropertySet vertexProperties, PropertySet edgeProperties) throws IOException {
		return getInstances(startTime, endTime, vertexProperties, edgeProperties, false);
	}

	@Override
	public Iterable<? extends ISubgraphInstance> getInstances(long startTime, long endTime, PropertySet vertexProperties, PropertySet edgeProperties, boolean reverse) throws IOException {
		throw new UnsupportedOperationException();
	}
	
	@Override
	public long getStartInstanceTime() throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public long getEndInstanceTime() throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean isRemoteVertex(long id) {
		if (!_template.containsVertex(id)) {
			throw new IllegalArgumentException();
		}

		return _remoteVertices.containsKey(id);
	}

	@Override
	public int getPartitionForRemoteVertex(long id) {
		Integer partition = _remoteVertices.get(id);
		if (partition == null) {
			throw new IllegalArgumentException();
		}

		return partition;
	}

	@Override
	public Long2IntMap getRemoteVertexMappings() {
		return _remoteVertices;
	}
}
