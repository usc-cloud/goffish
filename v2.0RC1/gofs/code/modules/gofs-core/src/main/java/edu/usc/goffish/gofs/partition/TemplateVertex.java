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

import it.unimi.dsi.fastutil.longs.*;

import edu.usc.goffish.gofs.*;
import edu.usc.goffish.gofs.graph.*;
import edu.usc.goffish.gofs.graph.impl.*;
import edu.usc.goffish.gofs.util.*;

public class TemplateVertex extends AbstractVertex<TemplateEdge> implements ITemplateVertex {

	private final long _id;

	private final Long2ObjectMap<TemplateEdge> _outEdges;

	public TemplateVertex(long id) {
		_id = id;
		_outEdges = new Long2ObjectAVLTreeMap<>();
	}

	@Override
	public long getId() {
		return _id;
	}

	@Override
	public boolean isRemote() {
		return false;
	}

	@Override
	public int getRemotePartitionId() {
		throw new UnsupportedOperationException();
	}

	@Override
	public long getRemoteSubgraphId() {
		throw new UnsupportedOperationException();
	}

	protected Iterable<TemplateEdge> outEdgesMutable() {
		return _outEdges.values();
	}

	protected Iterable<TemplateEdge> outEdgesUniqueMutable() {
		return UniqueEdgeIterable.fromTypedVertices(Collections.singletonList(this));
	}

	@Override
	public Iterable<TemplateEdge> outEdges() {
		return IterableUtils.unmodifiableIterable(outEdgesMutable());
	}

	@Override
	public Iterable<TemplateEdge> outEdgesUnique() {
		return IterableUtils.unmodifiableIterable(outEdgesUniqueMutable());
	}

	@Override
	public int outDegree() {
		return _outEdges.size();
	}

	@Override
	public boolean containsOutEdge(IEdge edge) {
		if (edge == null) {
			throw new IllegalArgumentException();
		}

		if (edge instanceof TemplateEdge) {
			return containsOutEdge(((TemplateEdge)edge).getId());
		}

		return false;
	}

	public boolean containsOutEdge(long edgeId) {
		return _outEdges.containsKey(edgeId);
	}

	@Override
	public boolean containsOutEdgeTo(IEdge edge, IVertex remote) {
		if (edge == null) {
			throw new IllegalArgumentException();
		}

		if (edge instanceof TemplateEdge) {
			return containsOutEdgeTo(((TemplateEdge)edge).getId(), remote);
		}

		return false;
	}

	public boolean containsOutEdgeTo(long edgeId, IVertex remote) {
		TemplateEdge edge = _outEdges.get(edgeId);
		if (edge != null) {
			return edge.getSink(this).equals(remote);
		}

		return false;
	}

	protected Iterable<TemplateEdge> outEdgesToMutable(IVertex remote) {
		return super.outEdgesToMutable(remote);
	}

	@Override
	protected boolean addOutEdge(TemplateEdge edge) {
		assert (edge.getSource().equals(this) || edge.getSink().equals(this));

		if (!_outEdges.containsKey(edge.getId())) {
			_outEdges.put(edge.getId(), edge);
			return true;
		}

		return false;
	}

	@Override
	protected boolean removeOutEdge(IEdge edge) {
		if (edge instanceof TemplateEdge) {
			return _outEdges.remove(((TemplateEdge)edge).getId()) != null;
		}

		return false;
	}

	@Override
	protected void clearOutEdges() {
		_outEdges.clear();
	}

	@Override
	public int compareTo(ITemplateVertex other) {
		return Long.compare(getId(), other.getId());
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
		if (!(obj instanceof TemplateVertex))
			return false;

		TemplateVertex other = (TemplateVertex)obj;
		if (_id != other._id) {
			return false;
		}

		return true;
	}

	@Override
	public String toString() {
		return getClass().getSimpleName() + "@" + _id;
	}
}
