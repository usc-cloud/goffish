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

package edu.usc.goffish.gofs.util.partitioning.streaming;

import java.util.*;

import edu.usc.goffish.gofs.graph.*;

public interface IObjectiveFunction {

	/**
	 * This method will be called once before the objective function is used to
	 * partition a graph to allow it to adjust any internal state.
	 * 
	 * @param numVertices
	 *            the number of vertices in the graph to be partitioned
	 * @param numPartitions
	 *            the number of partitions
	 */
	void reset(int numVertices, int numPartitions);

	/**
	 * This function should calculate using some heuristic the partition to
	 * assign the vertex too out of the given list of partitions. The
	 * implementation of this function MUST be idempotent.
	 * 
	 * @param vertex
	 *            the vertex to assign to a partition
	 * @param partitions
	 *            the choice of partitions to assign the vertex too
	 * @return the partition to assign the vertex too
	 */
	IStreamPartition choosePartition(IIdentifiableVertex vertex, List<IStreamPartition> partitions);
}
