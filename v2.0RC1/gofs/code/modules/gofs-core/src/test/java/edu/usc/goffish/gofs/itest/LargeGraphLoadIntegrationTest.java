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

import java.util.*;

import junit.framework.*;

import edu.usc.goffish.gofs.graph.*;
import edu.usc.goffish.gofs.graph.algorithms.*;

public class LargeGraphLoadIntegrationTest extends TestCase implements IntegrationTest {

	private IGraph<? extends IVertex, ? extends IEdge> _graph;

	@Override
	protected void setUp() throws Exception {
		super.setUp();

		_graph = GoogleGraphLoader.constructGoogleGraph();
	}

	@Override
	protected void tearDown() throws Exception {
		super.tearDown();
	}

	public void testLoad() {
		assertEquals(GoogleGraphLoader.NUM_VERTICES, _graph.numVertices());
		assertEquals(GoogleGraphLoader.NUM_EDGES, _graph.numEdges());

		Collection<? extends Collection<? extends IVertex>> splits = ConnectedComponents.findWeak(_graph);
		assertEquals(GoogleGraphLoader.NUM_SUBGRAPHS, splits.size());

		int largest_wcc = 0;
		for (Collection<? extends IVertex> split : splits) {
			if (split.size() > largest_wcc) {
				largest_wcc = split.size();
			}
		}

		assertEquals(GoogleGraphLoader.NUM_WCC_VERTICES, largest_wcc);
	}
}
