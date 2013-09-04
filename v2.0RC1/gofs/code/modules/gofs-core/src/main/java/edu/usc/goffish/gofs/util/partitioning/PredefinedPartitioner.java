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

package edu.usc.goffish.gofs.util.partitioning;

import java.io.*;

import edu.usc.goffish.gofs.graph.*;

public class PredefinedPartitioner implements IPartitioner {

	private final IPartitioning _partitioning;
	
	public PredefinedPartitioner(IPartitioning partitioning) {
		if (partitioning == null) {
			throw new IllegalArgumentException();
		}
		
		_partitioning = partitioning;
	}

	@Override
	public IPartitioning partition(IIdentifiableVertexGraph<? extends IIdentifiableVertex, ? extends IEdge> graph, int numPartitions) throws IOException {
		return _partitioning;
	}
}
