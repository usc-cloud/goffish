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
package edu.usc.goffish.gofs.graph;

import edu.usc.goffish.gofs.util.*;

public abstract class AbstractGraph<TVertex extends AbstractVertex<TEdge> & ITypedVertex<TEdge>, TEdge extends IEdge> implements IGraph<TVertex, TEdge> {

	@Override
	public abstract boolean isDirected();

	@Override
	public abstract Iterable<TVertex> vertices();

	@Override
	public abstract Iterable<TEdge> edges();

	@Override
	public int numVertices() {
		return IterableHelper.iterableCount(vertices());
	}

	@Override
	public long numEdges() {
		return IterableHelper.iterableCount(edges());
	}

	@Override
	public boolean containsVertex(IVertex vertex) {
		return IterableHelper.iterableContains(vertices(), vertex);
	}

	@Override
	public boolean containsEdge(IEdge edge) {
		return IterableHelper.iterableContains(edges(), edge);
	}
}
