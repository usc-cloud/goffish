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

import java.io.*;
import java.net.*;
import java.util.*;

/**
 * This interface represents a central authority which contains mappings from
 * partition ids to the locations of those partitions. How those locations are
 * represented is somewhat implementation dependent, but
 * {@link #getPartitionMapping(String, int)} contains some discussion of
 * conventional ways of achieving this.
 */
public interface INameNode {

	/**
	 * Retrieves a list of all the partitions for a graph with the given id.
	 * This information is a copy, and not a live view of the set of partitions.
	 * 
	 * @param graphId
	 *            the graph to retrieve partition ids for
	 * @return a list of all the partitions for a graph with the given id, or
	 *         null if the graph is unknown
	 * @throws IOException
	 */
	List<Integer> getPartitions(String graphId) throws IOException;

	/**
	 * Returns all the URIs that match the given graph and location. Matching is
	 * performed for every piece of the input location URI that is not null.
	 * I.e., if the host is non-null, the host must match, if the path is
	 * non-null, the path must match, etc... Matching is case-sensitive. The
	 * following URI pieces are matched: scheme, userinfo, host, port, path,
	 * query, fragment. For the path component of the URI, partial matching is
	 * allowed. I.e. a path of '/dir1' will match both '/dir1' and '/dir1/dir2'.
	 * 
	 * @param graphId
	 *            the graph to retrieve partition ids for
	 * @param locationToMatch
	 *            a URI which will match against every non-null piece of this
	 *            URI. Path matching allows for partial matching. The URI must
	 *            be non-opaque.
	 * @return a map of the graphs and partitions matching the query
	 * @throws IOException
	 */
	List<Integer> getMatchingPartitions(String graphId, URI locationToMatch) throws IOException;

	/**
	 * Retrieves a URI representing the specified partition. By convention, this
	 * generally represents the host, path, and UUID of the partition slice. The
	 * host and path are specified by the URI, and the UUID is attached as a
	 * string as the URI fragment. The UUID can thus be reconstructed through
	 * UUID.fromString(uri.getFragment()).
	 * 
	 * @param graphId
	 *            the graph id of the graph that contains the given partition
	 * @param partitionId
	 *            the partition id being queried for
	 * @return a URI representing the location of the partition
	 * @throws IOException
	 */
	URI getPartitionMapping(String graphId, int partitionId) throws IOException;

	/**
	 * Returns whether this name node knows of a mapping for the given partition
	 * in the given graph.
	 * 
	 * @param graphId
	 *            the graph id of the graph that contains the given partition
	 * @param partitionId
	 *            the partition id being queried for
	 * @return True if this name node knows of such a mapping, false otherwise
	 * @throws IOException
	 */
	boolean hasPartitionMapping(String graphId, int partitionId) throws IOException;

	/**
	 * Informs the name node of the existence of a mapping from partition id to
	 * partition location, which will be returned by any subsequent queries. The
	 * location URI may not be opaque.
	 * 
	 * @param graphId
	 *            the graph id of the graph that contains the given partition
	 * @param partitionId
	 *            the partition id being queried for
	 * @param location
	 *            the location of the given partition. See
	 *            {@link #getPartitionMapping(String, int)} for a discussion of
	 *            conventional formats.
	 * @throws IOException
	 */
	void putPartitionMapping(String graphId, int partitionId, URI location) throws IOException;

	/**
	 * Removes a mapping for the given partition from this name node.
	 * 
	 * @param graphId
	 *            the graph id of the graph that contains the given partition
	 * @param partitionId
	 *            the partition id being queried for
	 * @throws IOException
	 */
	void removePartitionMapping(String graphId, int partitionId) throws IOException;
}
