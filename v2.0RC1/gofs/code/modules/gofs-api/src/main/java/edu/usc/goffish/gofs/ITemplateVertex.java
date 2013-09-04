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

import edu.usc.goffish.gofs.graph.*;

public interface ITemplateVertex extends IUndirectedVertex, IDirectedVertex, IIdentifiableVertex, Comparable<ITemplateVertex> {

	@Override
	Iterable<? extends ITemplateEdge> outEdges();

	@Override
	Iterable<? extends ITemplateEdge> outEdgesUnique();

	@Override
	Iterable<? extends ITemplateEdge> outEdgesTo(IVertex remote);

	/**
	 * Returns true if this is a remote vertex, representing a vertex in some other partition.
	 * 
	 * @return true if this is a remote vertex
	 */
	boolean isRemote();

	/**
	 * Returns the partition this remote vertex belongs to. This is an optional API, and the result is only defined if
	 * this is in fact a remote vertex.
	 * 
	 * @return the partition this remote vertex belongs to
	 */
	int getRemotePartitionId();

	/**
	 * Returns the subgraph this remote vertex belongs to in the remote partition. This is an optional API, and the
	 * result is only defined if this is in fact a remote vertex.
	 * 
	 * @return the subgraph this remote vertex belongs to in the remote partition
	 */
	long getRemoteSubgraphId();
}
