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

package edu.usc.goffish.gofs;

import java.io.*;

/**
 * This interface represents a weakly connected component within a partition, and the associated instance data.
 * Conceptually, a subgraph consists of a template (the actual graph, vertices, and edges) and a set of instances
 * (properties associated with various vertices and edges over time). The template is always held in memory, the
 * instances are generally read from disk on demand, though specific implementations may cache at will.
 */
public interface ISubgraph {

	/**
	 * Returns the id of subgraph.
	 * 
	 * @return id of the subgraph
	 */
	long getId();

	/**
	 * Returns if the graph is directed.
	 * 
	 * @return true if the graph is directed, false otherwise
	 */
	boolean isDirected();

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
	 * Returns the subgraph template which contains the structure of this subgraph.
	 * 
	 * @return the subgraph template which contains the structure of this subgraph
	 */
	ISubgraphTemplate<? extends ITemplateVertex, ? extends ITemplateEdge> getTemplate();

	/**
	 * Returns an Iterable over all instances in this subgraph. Each ISubgraphInstance is expected to be loaded on
	 * demand. Instances will be evaluated and returned based on the given time range [startTime, endTime) and will
	 * return only the specified properties.
	 * 
	 * @param startTime
	 *            the beginning of the instance time range, inclusive
	 * @param endTime
	 *            the end of the instance time range, exclusive
	 * @param vertexProperties
	 *            the properties to retrieve for vertices
	 * @param edgeProperties
	 *            the properties to retrieve for edges
	 * @param reverse
	 *            whether to iterate forwards or backwards over instances
	 * @return An Iterable which will return instances matching the query, loaded on demand
	 * @throws IOException
	 */
	Iterable<? extends ISubgraphInstance> getInstances(long startTime, long endTime, PropertySet vertexProperties, PropertySet edgeProperties, boolean reverse) throws IOException;

	/**
	 * Convenience method which is identical to calling vertices() on the ISubgraphTemplate returned from
	 * {@link #getTemplate()}.
	 * 
	 * @return an iterable of the vertices in this subgraph
	 */
	Iterable<? extends ITemplateVertex> vertices();

	/**
	 * Returns an iterable of the remote vertices in this subgraph.
	 * 
	 * @return an iterable of the remote vertices in this subgraph
	 */
	Iterable<? extends ITemplateVertex> remoteVertices();

	/**
	 * Convenience method which is identical to calling edges() on the ISubgraphTemplate returned from
	 * {@link #getTemplate()}.
	 * 
	 * @return an iterable of the edges in this subgraph
	 */
	Iterable<? extends ITemplateEdge> edges();

	/**
	 * Convenience method which is identical to calling numVertices() on the ISubgraphTemplate returned from
	 * {@link #getTemplate()}.
	 * 
	 * @return the number of vertices in this subgraph
	 */
	int numVertices();

	/**
	 * Returns the number of remote vertices in this subgraph.
	 * 
	 * @return the number of remote vertices in this subgraph
	 */
	int numRemoteVertices();

	/**
	 * Convenience method which is identical to calling numEdges() on the ISubgraphTemplate returned from
	 * {@link #getTemplate()}.
	 * 
	 * @return the number of edges in this subgraph
	 */
	long numEdges();

	/**
	 * Convenience method which is identical to calling getVertex() on the ISubgraphTemplate returned from
	 * {@link #getTemplate()}.
	 * 
	 * @param vertexId
	 *            id of the vertex in question
	 * @return the vertex
	 */
	ITemplateVertex getVertex(long vertexId);

	/**
	 * Convenience method which is identical to calling containsVertex on the ISubgraphTemplate returned from
	 * {@link #getTemplate()}.
	 * 
	 * @param vertexId
	 *            id of the vertex in question
	 * @return true if the vertex is in this subgraph, false otherwise
	 */
	boolean containsVertex(long vertexId);

	/**
	 * Convenience method which is identical to calling getEdge on the ISubgraphTemplate returned from
	 * {@link #getTemplate()}.
	 * 
	 * @param edgeId
	 *            id of the edge in question
	 * @return the edge
	 */
	ITemplateEdge getEdge(long edgeId);

	/**
	 * Convenience method which is identical to calling containsEdge on the ISubgraphTemplate returned from
	 * {@link #getTemplate()}.
	 * 
	 * @param edgeId
	 *            id of the edge in question
	 * @return true if the edge is in this subgraph, false otherwise
	 */
	boolean containsEdge(long edgeId);
}
