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

import java.util.*;

import edu.usc.goffish.gofs.*;
import edu.usc.goffish.gofs.graph.*;
import edu.usc.goffish.gofs.graph.algorithms.*;
import edu.usc.goffish.gofs.util.*;

public class BasePartition extends AbstractCollection<ISubgraph> implements IPartition {

	public static final int INVALID_PARTITION = Integer.MIN_VALUE;
	
	private final int _id;
	private final boolean _directed;
	
	private final Collection<ISubgraph> _subgraphs;

	private final PropertySet _vertexProperties;
	private final PropertySet _edgeProperties;

	public BasePartition(int id, boolean directed, Collection<? extends ISubgraph> subgraphs, PropertySet vertexProperties, PropertySet edgeProperties) {
		if (id == INVALID_PARTITION) {
			throw new IllegalArgumentException();
		}
		if (subgraphs == null) {
			throw new IllegalArgumentException();
		}
		if (vertexProperties == null) {
			throw new IllegalArgumentException();
		}
		if (edgeProperties == null) {
			throw new IllegalArgumentException();
		}

		_id = id;
		_directed = directed;
		_subgraphs = new ArrayList<>(subgraphs);
		_vertexProperties = vertexProperties;
		_edgeProperties = edgeProperties;
	}

	public BasePartition(int id, IGraph<? extends TemplateVertex, ? extends TemplateEdge> partitionGraph, PropertySet vertexProperties, PropertySet edgeProperties, Map<Long, Integer> remoteVertices, ISubgraphFactory subgraphFactory) {
		if (partitionGraph == null) {
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
		if (subgraphFactory == null) {
			throw new IllegalArgumentException();
		}

		_id = id;
		_directed = partitionGraph.isDirected();
		_vertexProperties = vertexProperties;
		_edgeProperties = edgeProperties;

		// split into subgraphs
		Collection<? extends Collection<? extends TemplateVertex>> splits = ConnectedComponents.findWeak(partitionGraph);

		_subgraphs = new ArrayList<>(splits.size());

		// create subgraphs from split
		long subgraph_id = 0;
		for (Collection<? extends TemplateVertex> vertices : splits) {
			TemplateGraph template = new TemplateGraph(_directed);
			Long2IntMap subgraphRemoteVertices = new Long2IntOpenHashMap();

			for (TemplateVertex v : vertices) {
				template.addVertex(v);
				if (remoteVertices.containsKey(v.getId())) {
					subgraphRemoteVertices.put(v.getId(), remoteVertices.get(v.getId()).intValue());
				}
			}

			_subgraphs.add(subgraphFactory.createSubgraph(subgraph_id++, template, vertexProperties, edgeProperties, subgraphRemoteVertices));
		}
	}

	@Override
	public int getId() {
		return _id;
	}

	@Override
	public boolean isDirected() {
		return _directed;
	}

	@Override
	public int numVertices() {
		int count = 0;
		for (ISubgraph sg : _subgraphs) {
			count += sg.numVertices();
		}
		
		return count;
	}
	
	@Override
	public int numRemoteVertices() {
		int count = 0;
		for (ISubgraph sg : _subgraphs) {
			count += sg.getRemoteVertexMappings().size();
		}
		
		return count;
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
	public boolean containsVertex(long vertexId) {
		return getSubgraphForVertex(vertexId) != null;
	}

	@Override
	public ISubgraph getSubgraphForVertex(long vertexId) {
		for (ISubgraph sg : _subgraphs) {
			if (sg.getTemplate().containsVertex(vertexId)) {
				return sg;
			}
		}

		return null;
	}

	@Override
	public boolean containsSubgraph(long subgraphId) {
		return getSubgraph(subgraphId) != null;
	}

	@Override
	public ISubgraph getSubgraph(long subgraphId) {
		for (ISubgraph sg : _subgraphs) {
			if (sg.getId() == subgraphId) {
				return sg;
			}
		}

		return null;
	}

	@Override
	public Iterator<ISubgraph> iterator() {
		return new ReadOnlyIterator<>(_subgraphs.iterator());
	}

	@Override
	public int size() {
		return _subgraphs.size();
	}
}
