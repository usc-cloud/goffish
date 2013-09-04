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
import edu.usc.goffish.gofs.graph.*;
import edu.usc.goffish.gofs.graph.impl.*;
import edu.usc.goffish.gofs.util.*;

public class TemplateGraph extends AbstractGraph<TemplateVertex, TemplateEdge> implements ISubgraphTemplate<TemplateVertex, TemplateEdge> {

	private final boolean _directed;

	private final Long2ObjectMap<TemplateVertex> _vertexMap;

	public TemplateGraph(boolean directed) {
		_directed = directed;
		_vertexMap = new Long2ObjectOpenHashMap<>();
	}

	public TemplateGraph(boolean directed, int initialCapacity) {
		_directed = directed;
		_vertexMap = new Long2ObjectOpenHashMap<>(initialCapacity);
	}

	@Override
	public boolean isDirected() {
		return _directed;
	}

	@Override
	public Iterable<TemplateVertex> vertices() {
		return IterableUtils.unmodifiableIterable(_vertexMap.values());
	}

	@Override
	public Iterable<TemplateEdge> edges() {
		return UniqueEdgeIterable.fromTypedVertices(vertices());
	}

	@Override
	public int numVertices() {
		return _vertexMap.size();
	}

	@Override
	public TemplateVertex getVertex(long vertexId) {
		return _vertexMap.get(vertexId);
	}

	@Override
	public boolean containsVertex(long vertexId) {
		return _vertexMap.containsKey(vertexId);
	}

	@Override
	public boolean containsVertex(TemplateVertex vertex) {
		if (vertex == null) {
			throw new IllegalArgumentException();
		}

		return _vertexMap.containsKey(vertex.getId());
	}

	@Override
	public boolean containsVertex(IVertex vertex) {
		if (vertex instanceof TemplateVertex) {
			return containsVertex((TemplateVertex)vertex);
		}

		return false;
	}

	@Override
	public TemplateEdge getEdge(long edgeId) {
		// the time cost of this method is expensive enough we don't want to
		// make it easily usable. if you really require this functionality it
		// can be implemented by scanning through edges()
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean containsEdge(long edgeId) {
		// see comments on getEdge(long edgeId)
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean containsEdge(long edgeId, long edgeSourceId, long edgeSinkId) {
		TemplateVertex source = getVertex(edgeSourceId);
		if (source == null) {
			return false;
		}

		TemplateVertex sink = getVertex(edgeSinkId);
		if (sink == null) {
			return false;
		}

		if (_directed) {
			return source.containsOutEdgeTo(edgeId, sink);
		} else {
			return source.containsOutEdgeTo(edgeId, sink) && sink.containsOutEdgeTo(edgeId, source);
		}
	}

	@Override
	public boolean containsEdge(TemplateEdge edge) {
		if (edge == null) {
			throw new IllegalArgumentException();
		}

		return containsEdge(edge.getId(), edge.getSource().getId(), edge.getSink().getId());
	}

	@Override
	public boolean containsEdge(IEdge edge) {
		if (edge instanceof TemplateEdge) {
			return containsEdge((TemplateEdge)edge);
		}

		return false;
	}

	public boolean addVertex(TemplateVertex vertex) {
		if (vertex == null) {
			throw new IllegalArgumentException();
		}

		if (!_vertexMap.containsKey(vertex.getId())) {
			_vertexMap.put(vertex.getId(), vertex);
			return true;
		}

		return false;
	}

	public boolean removeVertex(IVertex vertex) {
		if (vertex instanceof TemplateVertex) {
			return removeVertex((TemplateVertex)vertex);
		}

		return false;
	}

	public boolean removeVertex(TemplateVertex vertex) {
		if (vertex == null) {
			throw new IllegalArgumentException();
		}

		TemplateVertex old = _vertexMap.remove(vertex.getId());
		if (old != null) {
			disconnectVertexFromGraph(old);
			return true;
		}

		return false;
	}

	public boolean connectEdge(TemplateEdge edge) {
		if (!containsVertex(edge.getSource().getId()) || !containsVertex(edge.getSink().getId())) {
			throw new IllegalArgumentException();
		}

		if (isDirected()) {
			return edge.getSource().addOutEdge(edge);
		} else {
			if (edge.getSource().equals(edge.getSink())) {
				// self edge only needs to be added once
				return edge.getSource().addOutEdge(edge);
			} else {
				// takes advantage of short circuiting (if first fails, don't even try second)
				return edge.getSource().addOutEdge(edge) && edge.getSink().addOutEdge(edge);
			}
		}
	}

	public boolean disconnectEdge(IEdge edge) {
		if (!(edge instanceof TemplateEdge)) {
			throw new IllegalArgumentException();
		}

		return disconnectEdge((TemplateEdge)edge);
	}

	public boolean disconnectEdge(TemplateEdge edge) {
		if (edge == null) {
			throw new IllegalArgumentException();
		}
		if (!containsVertex(edge.getSource()) || !containsVertex(edge.getSink())) {
			throw new IllegalArgumentException();
		}

		if (isDirected()) {
			return edge.getSource().removeOutEdge(edge);
		} else {
			boolean t = false;
			t = t || edge.getSource().removeOutEdge(edge);
			t = t || edge.getSink().removeOutEdge(edge);
			return t;
		}
	}

	public void clear() {
		for (TemplateVertex v : vertices()) {
			v.clearOutEdges();
		}

		_vertexMap.clear();
	}

	protected void disconnectVertexFromGraph(TemplateVertex vertex) {
		assert (vertex != null);

		if (isDirected()) {
			for (TemplateVertex v : vertices()) {
				if (v.equals(vertex)) {
					continue;
				}

				Iterator<TemplateEdge> it = v.outEdgesToMutable(vertex).iterator();
				while (it.hasNext()) {
					it.next();
					it.remove();
				}
			}
		} else {
			for (TemplateEdge e : vertex.outEdges()) {
				e.getSink(vertex).removeOutEdge(e);
			}
		}
	}

	@Override
	public String toString() {
		return _vertexMap.values().toString();
	}
}
