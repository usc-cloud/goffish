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
package edu.usc.goffish.gofs.itest;

import java.util.*;

import edu.usc.goffish.gofs.*;

public class GoogleGraphLoader {

	public static final int NUM_VERTICES = 875713;
	public static final long NUM_EDGES = 5105039;
	public static final int NUM_SUBGRAPHS = 2746;
	public static final int NUM_WCC_VERTICES = 855802;

	private static final String _file = "web-Google.txt";

	private GoogleGraphLoader() {
		throw new UnsupportedOperationException();
	}

	public static TemplateGraph constructGoogleGraph() {
		Map<String, Long> vertexIds = new HashMap<>();
		
		TemplateGraph graph = new TemplateGraph(true);

		try (Scanner is_graph = new Scanner(ClassLoader.getSystemResourceAsStream(_file))) {
			long vertex_id = 1;
			long edge_id = 1;

			while (is_graph.hasNext()) {
				String t1 = is_graph.next();
				if (t1.startsWith("#")) {
					is_graph.nextLine();
					continue;
				}
				String t2 = is_graph.next();

				Long v1 = vertexIds.get(t1);
				if (v1 == null) {
					v1 = new Long(vertex_id++);
					vertexIds.put(t1, v1);
				}
				Long v2 = vertexIds.get(t2);
				if (v2 == null) {
					v2 = new Long(vertex_id++);
					vertexIds.put(t2, v2);
				}

				// get vertex 1
				TemplateVertex vertex1 = graph.getVertex(v1);
				if (vertex1 == null) {
					vertex1 = new TemplateVertex(v1);
					graph.addVertex(vertex1);
				}

				// get vertex 2
				TemplateVertex vertex2 = graph.getVertex(v2);
				if (vertex2 == null) {
					vertex2 = new TemplateVertex(v2);
					graph.addVertex(vertex2);
				}

				// connect vertices
				graph.connectEdge(new TemplateEdge(edge_id++, vertex1, vertex2));
			}
		}

		return graph;
	}
}
