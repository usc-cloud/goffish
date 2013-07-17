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

package edu.usc.goffish.gofs.slice;

import java.io.*;
import java.util.*;

import edu.usc.goffish.gofs.*;
import edu.usc.goffish.gofs.partition.*;

class SliceSubgraph extends BaseSubgraph {

	private final SliceManager _sliceManager;

	public SliceSubgraph(long id, ISubgraphTemplate template, SliceManager sliceManager, PropertySet vertexProperties, PropertySet edgeProperties, Map<Long, Integer> remoteVertices) {
		super(id, template, vertexProperties, edgeProperties, remoteVertices);

		if (sliceManager == null) {
			throw new IllegalArgumentException();
		}

		_sliceManager = sliceManager;
	}

	@Override
	public Iterable<? extends ISubgraphInstance> getInstances(long startTime, long endTime, PropertySet vertexProperties, PropertySet edgeProperties, boolean reverse) throws IOException {
		if (startTime > endTime) {
			throw new IllegalArgumentException();
		}
		if (vertexProperties == null) {
			throw new IllegalArgumentException();
		}
		if (edgeProperties == null) {
			throw new IllegalArgumentException();
		}
		if (!getVertexProperties().containsAll(vertexProperties)) {
			throw new IllegalArgumentException();
		}
		if (!getEdgeProperties().containsAll(edgeProperties)) {
			throw new IllegalArgumentException();
		}

		return _sliceManager.readInstances(reverse, this, startTime, endTime, vertexProperties, edgeProperties);
	}
	
	@Override
	public long getStartInstanceTime() throws IOException {
		return _sliceManager.getInstancesFirstTime(getId());
	}

	@Override
	public long getEndInstanceTime() throws IOException {
		return _sliceManager.getInstancesLastTime(getId());
	}
}
