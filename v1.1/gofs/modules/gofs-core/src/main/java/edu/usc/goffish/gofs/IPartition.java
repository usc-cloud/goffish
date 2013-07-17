/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law
 * or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */

package edu.usc.goffish.gofs;

import java.util.*;

/**
 * A partition represents a subset of the vertices and edges in a graph. A
 * partition is further divided into subgraphs, where each subgraph is a weakly
 * connected component within the partition graph. As a subset of vertices, a
 * partition may contain edges that connect to vertices that are not in this
 * partition. This is dealt with through the notion of remote vertices. A remote
 * vertex is a vertex in the partition which actually represents a vertex owned
 * by another partition. Each subgraph contains mappings for each remote vertex
 * it contains to the partition that owns that remote vertex. A remote vertex is
 * still a proper vertex, in that if queried, the subgraph believes it contains
 * that vertex. However, a remote vertex will only contain links into and out of
 * this partition, it does not contain any links further into the partition that
 * actually owns it. The expected use case is that if a remote vertex is found
 * while traversing links, the partition that owns it will be identified and
 * communicated with, and that partition can supply the vertex with the same id
 * it contains, which is the real vertex.
 */
public interface IPartition extends Collection<ISubgraph> {

	/**
	 * Returns the id of this partition.
	 * 
	 * @return the id of this partition
	 */
	int getId();

	/**
	 * Returns whether this partition contains directed graphs or not.
	 * 
	 * @return true if this partition contains directed graphs, and false if
	 *         this partition contains undirected graphs
	 */
	boolean isDirected();

	/**
	 * Returns the total number of vertex objects in this partition, which
	 * includes both vertices this partition owns, and remote vertices in other
	 * partitions. The number of vertices this partition owns is
	 * {@link #numVertices()} - {@link #numRemoteVertices()}.
	 * 
	 * @return the total number vertex objects in this partition
	 */
	int numVertices();

	/**
	 * Returns the total number of remote vertex objects in this partition. The
	 * number of vertices this partition owns is {@link #numVertices()} -
	 * {@link #numRemoteVertices()}.
	 * 
	 * @return the total number of remote vertex objects in this partition.
	 */
	int numRemoteVertices();

	/**
	 * Returns the set of vertex properties associated with this graph.
	 * 
	 * @return the set of vertex properties associated with this graph
	 */
	PropertySet getVertexProperties();

	/**
	 * Returns the set of edge properties associated with this graph.
	 * 
	 * @return the set of edge properties associated with this graph
	 */
	PropertySet getEdgeProperties();
	
	/**
	 * Returns whether or not this partition has a subgraph that knows of the
	 * specified vertex. The subgraph does not necessarily own the vertex, it
	 * may only know of it as a remote vertex which is owned by some other
	 * subgraph, but it will still return true if the vertex exists as a remote
	 * vertex in this partition. See {@link #ownsVertex(long)} to find out if a
	 * vertex is owned by this partition.
	 * 
	 * @param vertexId
	 *            the vertex to check subgraphs for
	 * @return true if this partition has a subgraph that contains the specified
	 *         vertex
	 */
	boolean containsVertex(long vertexId);

	/**
	 * Returns the subgraph that contains the given vertex id or null if there
	 * is no such subgraph. In most implementations this will likely be a scan
	 * linear in time by the number of subgraphs.
	 * 
	 * @param vertexId
	 *            the vertex to check subgraphs for
	 * @return the subgraph that contains this vertex
	 */
	ISubgraph getSubgraphForVertex(long vertexId);

	/**
	 * Returns true if this partition contains the specified subgraph.
	 * 
	 * @param subgraphId
	 *            the subgraph to check for
	 * @return true if this partition contains the specified subgraph
	 */
	boolean containsSubgraph(long subgraphId);

	/**
	 * Returns the specified subgraph.
	 * 
	 * @param subgraphId
	 *            the subgraph to retrieve
	 * @return the subgraph with the given id
	 */
	ISubgraph getSubgraph(long subgraphId);
}
