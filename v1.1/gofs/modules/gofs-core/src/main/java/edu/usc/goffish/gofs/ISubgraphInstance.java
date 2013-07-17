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
package edu.usc.goffish.gofs;

import java.util.*;

/**
 * This represents a single instance of data associated with a subgraph. It has
 * methods to retrieve information about the instance as well as to query
 * properties for a specific vertex or edge.
 */
public interface ISubgraphInstance {

	/**
	 * Returns the instance id.
	 * 
	 * @return the instance id
	 */
	long getId();

	/**
	 * Returns the template associated with this instance.
	 * 
	 * @return the template associated with this instance
	 */
	ISubgraphTemplate getTemplate();

	/**
	 * Returns the start time of this instance.
	 * 
	 * @return the start time of this instance
	 */
	long getTimestampStart();

	/**
	 * Returns the end time of this instance.
	 * 
	 * @return the end time of this instance
	 */
	long getTimestampEnd();

	/**
	 * The set of vertex properties this instance contains data for.
	 * 
	 * @return set of vertex properties this instance contains data for
	 */
	PropertySet getVertexProperties();

	/**
	 * The set of edge properties this instance contains data for.
	 * 
	 * @return set of edge properties this instance contains data for
	 */
	PropertySet getEdgeProperties();

	/**
	 * Returns a representation of the property values for a specific vertex.
	 * 
	 * @param id
	 *            the vertex to return property values for
	 * @return property values for a specific vertex
	 */
	ISubgraphObjectProperties getPropertiesForVertex(long id);

	/**
	 * Returns a collection of the property values for every vertex.
	 * 
	 * @return a collection of the property values for every vertex
	 */
	Collection<? extends ISubgraphObjectProperties> getPropertiesForVertices();

	/**
	 * Returns a representation of the property values for a specific edge.
	 * 
	 * @param id
	 *            the edge to return property values for
	 * @return property edge for a specific vertex
	 */
	ISubgraphObjectProperties getPropertiesForEdge(long id);

	/**
	 * Returns a collection of the property values for every edge.
	 * 
	 * @return a collection of the property values for every edge
	 */
	Collection<? extends ISubgraphObjectProperties> getPropertiesForEdges();
}
