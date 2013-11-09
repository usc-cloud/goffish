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

final class EdgeMatchingIterable<TEdge extends IEdge> implements Iterable<TEdge> {

	private final ITypedVertex<TEdge> _vertex;
	private final IVertex _toMatch;

	public EdgeMatchingIterable(ITypedVertex<TEdge> vertex, IVertex toMatch) {
		if (vertex == null) {
			throw new IllegalArgumentException();
		}
		if (toMatch == null) {
			throw new IllegalArgumentException();
		}

		_vertex = vertex;
		_toMatch = toMatch;
	}

	@Override
	public Iterator<TEdge> iterator() {
		return new EdgeMatchingIterator<>(_vertex, _toMatch);
	}

	static final class EdgeMatchingIterator<TEdge extends IEdge> extends AbstractWrapperIterator<TEdge> {

		private final IVertex _vertex;
		private final IVertex _toMatch;
		private final Iterator<? extends TEdge> _edgeIterator;

		public EdgeMatchingIterator(ITypedVertex<? extends TEdge> vertex, IVertex toMatch) {
			assert (vertex != null);
			assert (toMatch != null);

			_vertex = vertex;
			_toMatch = toMatch;
			_edgeIterator = vertex.outEdges().iterator();
		}

		@Override
		public void remove() {
			_edgeIterator.remove();
		}

		@Override
		protected TEdge advanceToNext() {
			while (_edgeIterator.hasNext()) {
				TEdge e = _edgeIterator.next();
				if (e.getSink(_vertex).equals(_toMatch) && e.getSource(_toMatch).equals(_vertex)) {
					return e;
				}
			}

			return null;
		}
	}
}
