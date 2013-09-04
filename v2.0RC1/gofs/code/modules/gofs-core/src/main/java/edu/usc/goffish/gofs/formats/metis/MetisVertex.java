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

package edu.usc.goffish.gofs.formats.metis;

import java.util.*;

import edu.usc.goffish.gofs.graph.*;
import edu.usc.goffish.gofs.graph.impl.*;
import edu.usc.goffish.gofs.util.*;

public class MetisVertex extends AbstractVertex<MetisEdge> implements IUndirectedVertex, IIdentifiableVertex, Comparable<MetisVertex> {

	private final long _id;

	private final List<MetisEdge> _edges;

	MetisVertex(long id) {
		_id = id;
		_edges = new LinkedList<>();
	}

	public long getId() {
		return _id;
	}

	@Override
	public Iterable<MetisEdge> outEdges() {
		return IterableUtils.unmodifiableIterable(_edges);
	}

	@Override
	public Iterable<MetisEdge> outEdgesUnique() {
		return IterableUtils.unmodifiableIterable(UniqueEdgeIterable.fromTypedVertices(Collections.singletonList(this)));
	}

	@Override
	protected boolean addOutEdge(MetisEdge edge) {
		assert (edge.getSink() == this || edge.getSource() == this);
		return _edges.add(edge);
	}

	@Override
	protected boolean removeOutEdge(IEdge edge) {
		throw new UnsupportedOperationException();
	}

	@Override
	protected void clearOutEdges() {
		throw new UnsupportedOperationException();
	}

	@Override
	public int compareTo(MetisVertex other) {
		return Long.compare(_id, other._id);
	}

	@Override
	public final int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int)(_id ^ (_id >>> 32));
		return result;
	}

	@Override
	public final boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (!(obj instanceof MetisVertex))
			return false;

		MetisVertex other = (MetisVertex)obj;
		if (_id != other._id) {
			return false;
		}

		return true;
	}

	@Override
	public String toString() {
		return getClass().getSimpleName() + _id;
	}
}
