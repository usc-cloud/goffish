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

package edu.usc.goffish.gofs.itest;

import java.io.*;

import edu.usc.goffish.gofs.*;
import edu.usc.goffish.gofs.partition.*;

public class TestSubgraph extends BaseSubgraph {

	public TestSubgraph(long id, ISubgraphTemplate<TemplateVertex, TemplateEdge> template, PropertySet vertexProperties, PropertySet edgeProperties) {
		super(id, template, vertexProperties, edgeProperties);
	}

	@Override
	public Iterable<ISubgraphInstance> getInstances() throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public Iterable<ISubgraphInstance> getInstances(long startTime, long endTime, PropertySet vertexProperties, PropertySet edgeProperties) throws IOException {
		throw new UnsupportedOperationException();
	}
}
