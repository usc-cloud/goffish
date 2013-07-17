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
package edu.usc.goffish.gofs.subgraph;

import java.util.*;

import junit.framework.*;

import edu.usc.goffish.gofs.*;

public class TemplateGraphTest extends TestCase {

	private TemplateGraph _graph;

	private List<TemplateVertex> _vertices;
	private List<TemplateEdge> _edges;

	@Override
	protected void setUp() throws Exception {
		super.setUp();
		
		_vertices = new ArrayList<>(4);
		_edges = new ArrayList<>(5);

		_graph = new TemplateGraph(true);

		TemplateVertex v1 = new TemplateVertex(1);
		TemplateVertex v2 = new TemplateVertex(2);
		TemplateVertex v3 = new TemplateVertex(3);
		TemplateVertex v4 = new TemplateVertex(4);

		_vertices.add(v1);
		_graph.addVertex(v1);
		_vertices.add(v2);
		_graph.addVertex(v2);
		_vertices.add(v3);
		_graph.addVertex(v3);
		_vertices.add(v4);
		_graph.addVertex(v4);

		TemplateEdge e1 = new TemplateEdge(1, v1, v2);
		_edges.add(e1);
		_graph.connectEdge(e1);

		TemplateEdge e2 = new TemplateEdge(2, v2, v3);
		_edges.add(e2);
		_graph.connectEdge(e2);

		TemplateEdge e3 = new TemplateEdge(3, v3, v2);
		_edges.add(e3);
		_graph.connectEdge(e3);

		TemplateEdge e4 = new TemplateEdge(4, v3, v4);
		_edges.add(e4);
		_graph.connectEdge(e4);

		TemplateEdge e5 = new TemplateEdge(5, v4, v3);
		_edges.add(e5);
		_graph.connectEdge(e5);
	}

	@Override
	protected void tearDown() throws Exception {
		super.tearDown();
	}

	public void testDirectionality() {
		assertEquals(true, _graph.isDirected());
	}

	public void testEdges() {
		TreeSet<TemplateEdge> testSet = new TreeSet<>();

		for (TemplateEdge e : _graph.edges()) {
			assertTrue(_edges.contains(e));
			assertTrue(testSet.add(e));
		}

		assertEquals(_edges.size(), testSet.size());
	}

	public void testNumEdges() {
		assertEquals(_edges.size(), _graph.numEdges());
	}

	public void testVertices() {
		TreeSet<TemplateVertex> testSet = new TreeSet<>();

		for (TemplateVertex v : _graph.vertices()) {
			assertTrue(_vertices.contains(v));
			assertTrue(testSet.add(v));
		}

		assertEquals(_vertices.size(), testSet.size());
	}

	public void testNumVertices() {
		assertEquals(_vertices.size(), _graph.numVertices());
	}

	public void testContainsVertex() {
		for (TemplateVertex v : _vertices) {
			assertTrue(_graph.containsVertex(v));
		}

		TemplateVertex v = new TemplateVertex(-1);
		assertFalse(_graph.containsVertex(v));
	}
	
	public void testGetVertexId() {
		for (TemplateVertex v : _vertices) {
			assertTrue(v.equals(_graph.getVertex(v.getId())));
		}
	}
	
	public void testContainsVertexId() {
		for (TemplateVertex v : _vertices) {
			assertTrue(_graph.containsVertex(v.getId()));
		}

		assertFalse(_graph.containsVertex(-1));
	}

	public void testContainsEdge() {
		for (TemplateEdge e : _edges) {
			assertTrue(_graph.containsEdge(e));
		}

		TemplateVertex v3 = _vertices.get(2);
		TemplateVertex v4 = _vertices.get(3);
		
		TemplateEdge e = new TemplateEdge(-1, v4, v3);
		assertFalse(_graph.containsEdge(e));
		
		e = new TemplateEdge(5, v3, v4);
		assertFalse(_graph.containsEdge(e));
	}
}
