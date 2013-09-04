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

package edu.usc.goffish.gofs.graph.algorithms;

import java.util.*;

import junit.framework.*;

import edu.usc.goffish.gofs.partition.*;

public class ConnectedComponentsTest extends TestCase {

	private TemplateGraph _graph;

	private LinkedList<TemplateVertex> _vertices1;
	private LinkedList<TemplateVertex> _vertices2;

	@Override
	protected void setUp() throws Exception {
		_vertices1 = new LinkedList<TemplateVertex>();
		_vertices2 = new LinkedList<TemplateVertex>();

		_graph = new TemplateGraph(true);

		TemplateVertex v1 = new TemplateVertex(1);
		TemplateVertex v2 = new TemplateVertex(2);
		TemplateVertex v3 = new TemplateVertex(3);
		TemplateVertex v4 = new TemplateVertex(4);

		_vertices1.add(v1);
		_graph.addVertex(v1);
		_vertices1.add(v2);
		_graph.addVertex(v2);

		TemplateEdge e1 = new TemplateEdge(1, v1, v2);
		_graph.connectEdge(e1);

		_vertices2.add(v3);
		_graph.addVertex(v3);
		_vertices2.add(v4);
		_graph.addVertex(v4);

		TemplateEdge e2 = new TemplateEdge(2, v3, v4);
		_graph.connectEdge(e2);
	}

	public void testSplit() {
		Collection<? extends Collection<TemplateVertex>> splits = ConnectedComponents.findWeak(_graph);

		for (Collection<TemplateVertex> split : splits) {
			if (split.containsAll(_vertices1)) {
				assertTrue(split.size() == _vertices1.size());
			} else if (split.containsAll(_vertices2)) {
				assertTrue(split.size() == _vertices2.size());
			} else {
				fail("Subgraph split does not match any predetermined split.");
			}
		}
	}

}
