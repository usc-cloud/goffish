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

package edu.usc.goffish.gofs.util;

import edu.usc.goffish.gofs.graph.*;
import edu.usc.goffish.gofs.util.partitioning.*;

public final class PartitioningHelper {
	
	private PartitioningHelper() {
		throw new UnsupportedOperationException();
	}
	
	public static long calculateEdgeCuts(IPartitioning partitioning, IGraph<? extends IIdentifiableVertex, ? extends IEdge> graph) {
		long edgeCuts = 0;
		for (IEdge edge : graph.edges()) {
			Integer sourcePartition = partitioning.get(((IIdentifiableVertex)edge.getSource()).getId());
			Integer sinkPartition = partitioning.get(((IIdentifiableVertex)edge.getSink()).getId());
			
			if (sourcePartition == null || sinkPartition == null) {
				// vertex not found in partitioning
				throw new IllegalStateException();
			}
			
			if (!sourcePartition.equals(sinkPartition)) {
				edgeCuts++;
			}
		}
		
		return edgeCuts;
	}
}
