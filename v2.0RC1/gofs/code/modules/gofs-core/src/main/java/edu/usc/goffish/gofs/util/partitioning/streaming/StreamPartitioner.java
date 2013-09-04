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

package edu.usc.goffish.gofs.util.partitioning.streaming;

import java.io.*;
import java.util.concurrent.*;

import edu.usc.goffish.gofs.graph.*;
import edu.usc.goffish.gofs.util.partitioning.*;

/**
 * This implementation of a streaming graph partitioning algorithm is based on
 * work by Isabelle Stanton and Gabriel Kliot. The paper detailing their work is
 * referenced below. <br/>
 * <br/>
 * Isabelle Stanton and Gabriel Kliot.
 * <em>Streaming graph partitioning for large distributed graphs.</em>
 * Proceedings of the 18th ACM SIGKDD international conference on Knowledge
 * discovery and data mining, August 12-16, 2012, Beijing, China
 * 
 */
@Deprecated
public class StreamPartitioner implements IPartitioner {

	private final IObjectiveFunction _objectiveFunction;

	public  StreamPartitioner(IObjectiveFunction objectiveFunction) {
		if (objectiveFunction == null) {
			throw new IllegalArgumentException();
		}

		_objectiveFunction = objectiveFunction;
	}

	@Override
	public IPartitioning partition(IIdentifiableVertexGraph<? extends IIdentifiableVertex, ? extends IEdge> graph, int numPartitions) throws IOException {
		_objectiveFunction.reset(graph.numVertices(), numPartitions);
		
		try {
			return new StreamPartitioningMaster(graph.numVertices(), numPartitions, _objectiveFunction, graph.vertices()).run();
		} catch (InterruptedException | ExecutionException e) {
			throw new IOException(e);
		}
	}
}
