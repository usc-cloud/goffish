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

import java.io.*;
import java.util.*;

import edu.usc.goffish.gofs.*;
import edu.usc.goffish.gofs.util.*;
import edu.usc.goffish.gofs.util.IterableUtils.IteratorFilter;

public abstract class BaseSubgraph implements ISubgraph {

	public static final long INVALID_SUBGRAPH = -1;

	private final long _id;
	private final ISubgraphTemplate<? extends TemplateVertex, ? extends TemplateEdge> _template;

	private final PropertySet _vertexProperties;
	private final PropertySet _edgeProperties;

	protected BaseSubgraph(long id, ISubgraphTemplate<? extends TemplateVertex, ? extends TemplateEdge> template, PropertySet vertexProperties, PropertySet edgeProperties) {
		if (id == INVALID_SUBGRAPH) {
			throw new IllegalArgumentException();
		}
		if (template == null) {
			throw new IllegalArgumentException();
		}
		if (vertexProperties == null) {
			throw new IllegalArgumentException();
		}
		if (edgeProperties == null) {
			throw new IllegalArgumentException();
		}

		_id = id;
		_template = template;

		_vertexProperties = vertexProperties;
		_edgeProperties = edgeProperties;
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
	public Iterable<? extends ITemplateVertex> remoteVertices() {
		return IterableUtils.<TemplateVertex>filterIterable(vertices(), new IteratorFilter<TemplateVertex>() {
			@Override
			public boolean filter(TemplateVertex e) {
				return e.isRemote();
			}
		});
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
	public int numRemoteVertices() {
		return IterableUtils.iterableCount(remoteVertices());
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
	public ISubgraphTemplate<? extends TemplateVertex, ? extends TemplateEdge> getTemplate() {
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
	public String toString() {
		return getClass().getName() + "[id=" + getId() + ", directed=" + _template.isDirected() + ", numVertices=" + numVertices() + "]";
	}

	public static class SubgraphVertexCountComparator implements Comparator<ISubgraph> {

		@Override
		public int compare(ISubgraph o1, ISubgraph o2) {
			return o1.numVertices() - o2.numVertices();
		}
	}
}
