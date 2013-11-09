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

import java.util.*;

import edu.usc.goffish.gofs.*;
import edu.usc.goffish.gofs.util.*;

public class Partition extends AbstractCollection<ISubgraph> implements IPartition {

	public static final int INVALID_PARTITION = Integer.MIN_VALUE;

	private final int _id;
	private final boolean _directed;

	private final Collection<ISubgraph> _subgraphs;
	private final Iterable<ISubgraph> _sortedSubgraphs;

	private final PropertySet _vertexProperties;
	private final PropertySet _edgeProperties;

	public Partition(int id, boolean directed, Collection<? extends ISubgraph> subgraphs, PropertySet vertexProperties, PropertySet edgeProperties) {
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

		// sorting subgraphs largest to smallest helps perf when searching subgraphs
		ISubgraph[] sortedSubgraphs = subgraphs.toArray(new ISubgraph[subgraphs.size()]);
		Arrays.sort(sortedSubgraphs, Collections.reverseOrder(new BaseSubgraph.SubgraphVertexCountComparator()));
		_sortedSubgraphs = Arrays.asList(sortedSubgraphs);

		_vertexProperties = vertexProperties;
		_edgeProperties = edgeProperties;
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
			count += sg.numRemoteVertices();
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
		for (ISubgraph sg : _sortedSubgraphs) {
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
