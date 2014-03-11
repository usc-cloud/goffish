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

import edu.usc.goffish.gofs.graph.*;
import edu.usc.goffish.gofs.util.*;

public abstract class AbstractVertex<TEdge extends IEdge> implements ITypedVertex<TEdge> {

	@Override
	public abstract Iterable<TEdge> outEdges();

	@Override
	public int outDegree() {
		return IterableUtils.iterableCount(outEdges());
	}

	@Override
	public boolean containsOutEdge(IEdge edge) {
		if (edge == null) {
			throw new IllegalArgumentException();
		}

		return IterableUtils.iterableContains(outEdges(), edge);
	}

	protected Iterable<TEdge> outEdgesToMutable(IVertex remote) {
		return new EdgeMatchingIterable<>(this, remote);
	}

	@Override
	public Iterable<TEdge> outEdgesTo(IVertex remote) {
		return IterableUtils.unmodifiableIterable(outEdgesToMutable(remote));
	}

	@Override
	public int outDegreeTo(IVertex remote) {
		return IterableUtils.iterableCount(outEdgesTo(remote));
	}

	@Override
	public boolean containsOutEdgeTo(IVertex remote) {
		return outEdgesTo(remote).iterator().hasNext();
	}

	@Override
	public boolean containsOutEdgeTo(IEdge edge, IVertex remote) {
		if (edge == null) {
			throw new IllegalArgumentException();
		}

		return IterableUtils.iterableContains(outEdgesTo(remote), edge);
	}

	protected abstract boolean addOutEdge(TEdge edge);

	protected abstract boolean removeOutEdge(IEdge edge);

	protected abstract void clearOutEdges();
}
