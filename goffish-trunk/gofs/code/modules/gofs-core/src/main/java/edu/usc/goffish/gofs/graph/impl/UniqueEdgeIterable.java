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

package edu.usc.goffish.gofs.graph.impl;

import java.util.*;

import edu.usc.goffish.gofs.graph.*;
import edu.usc.goffish.gofs.util.*;

public final class UniqueEdgeIterable<TEdge extends IEdge> implements Iterable<TEdge> {

	private final Iterable<? extends IVertex> _vertices;
	
	private UniqueEdgeIterable(Iterable<? extends IVertex> vertices) {
		if (vertices == null) {
			throw new IllegalArgumentException();
		}
		
		_vertices = vertices;
	}
	
	public static <TVertex extends ITypedVertex<TEdge> & IUndirectedVertex, TEdge extends IEdge> Iterable<TEdge> fromTypedVertices(Iterable<TVertex> vertices) {
		return new UniqueEdgeIterable<>(vertices);
	}
	
	public static <TVertex extends IVertex> Iterable<IEdge> fromUntypedVertices(Iterable<TVertex> vertices) {
		return new UniqueEdgeIterable<IEdge>(vertices);
	}
	
	@Override
	public Iterator<TEdge> iterator() {
		return new EdgeIterator<TEdge>(_vertices.iterator());
	}

	private static final class EdgeIterator<TEdge extends IEdge> extends AbstractWrapperIterator<TEdge> {

		private final Iterator<? extends IVertex> _vertexIterator;

		private IVertex _currentVertex;
		private Iterator<? extends IEdge> _edgeIterator;

		public EdgeIterator(Iterator<? extends IVertex> vertexIterator) {
			_vertexIterator = vertexIterator;
			_currentVertex = null;
			_edgeIterator = null;

			if (_vertexIterator.hasNext()) {
				_currentVertex = _vertexIterator.next();
				_edgeIterator = _currentVertex.outEdges().iterator();
			}
		}

		@Override
		public void remove() {
			if (_edgeIterator == null) {
				throw new IllegalStateException();
			}
			
			_edgeIterator.remove();
		}

		@SuppressWarnings("unchecked")
		@Override
		protected TEdge advanceToNext() {
			do {
				while (!_edgeIterator.hasNext() && _vertexIterator.hasNext()) {
					_currentVertex = _vertexIterator.next();
					_edgeIterator = _currentVertex.outEdges().iterator();
				}

				while (_edgeIterator.hasNext()) {
					TEdge next = (TEdge)_edgeIterator.next();

					/*
					 * we don't want to repeat edges in spite of the fact that
					 * an undirected edge appears in the out list of both source
					 * and sink vertices. so we take advantage of the fact that
					 * the for undirected edges, the source will not correspond
					 * to the vertex we came from for one of the vertices, and
					 * we avoid iterating this edge twice.
					 */

					if (next.getSource().equals(_currentVertex)) {
						return next;
					}
				}
			} while (_vertexIterator.hasNext());
			
			return null;
		}
	}
}
