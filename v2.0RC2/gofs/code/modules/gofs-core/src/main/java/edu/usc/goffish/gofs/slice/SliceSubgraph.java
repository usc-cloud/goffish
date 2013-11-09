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

package edu.usc.goffish.gofs.slice;

import java.io.*;
import java.util.*;

import edu.usc.goffish.gofs.*;
import edu.usc.goffish.gofs.partition.*;
import edu.usc.goffish.gofs.util.*;

final class SliceSubgraph extends BaseSubgraph {

	private final SliceManager _sliceManager;

	private Collection<? extends ITemplateVertex> _cachedRemoteVertices;
	
	public SliceSubgraph(long id, ISubgraphTemplate<TemplateVertex, TemplateEdge> template, SliceManager sliceManager, PropertySet vertexProperties, PropertySet edgeProperties) {
		super(id, template, vertexProperties, edgeProperties);

		if (sliceManager == null) {
			throw new IllegalArgumentException();
		}

		_sliceManager = sliceManager;
	}
	
	@Override
	public Collection<? extends ITemplateVertex> remoteVertices() {
		if (_cachedRemoteVertices == null) {
			_cachedRemoteVertices = Collections.unmodifiableCollection(IterableUtils.toList(super.remoteVertices()));
		}
		
		return _cachedRemoteVertices;
	}

	@Override
	public int numRemoteVertices() {
		return remoteVertices().size();
	}

	@Override
	public Iterable<? extends ISubgraphInstance> getInstances(long startTime, long endTime, PropertySet vertexProperties, PropertySet edgeProperties, boolean reverse) throws IOException {
		if (!getVertexProperties().containsAll(vertexProperties)) {
			throw new IllegalArgumentException();
		}
		if (!getEdgeProperties().containsAll(edgeProperties)) {
			throw new IllegalArgumentException();
		}

		return _sliceManager.getInstances((ISubgraph)this, startTime, endTime, vertexProperties, edgeProperties, reverse);
	}
}
