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

public interface IIdentifiableEdgeGraph<TVertex extends IVertex, TEdge extends IIdentifiableEdge> extends IGraph<TVertex, TEdge> {

	/**
	 * Returns the edge with the specified id.
	 * 
	 * @param edgeId
	 *            the id of the desired edge
	 * @return the edge with the specified id
	 */
	TEdge getEdge(long edgeId);

	/**
	 * Checks if an edge with the specified id if may be found within this
	 * subgraph.
	 * 
	 * @param edgeId
	 *            the edge id of the desired edge
	 * @return true if the edge is within this subgraph, false otherwise
	 */
	boolean containsEdge(long edgeId);
}
