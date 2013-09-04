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

package edu.usc.goffish.gofs.tools.deploy;

import java.io.*;
import java.net.*;
import java.nio.file.*;
import java.util.*;

import edu.usc.goffish.gofs.partition.*;
import edu.usc.goffish.gofs.slice.*;

public class DirectWritePartitionDistributer implements IPartitionDistributer {

	private final ISliceSerializer _serializer;
	private final int _instancesGroupingSize;
	private final int _numSubgraphBins;

	public DirectWritePartitionDistributer() {
		this(new JavaSliceSerializer(), 1, -1);
	}

	public DirectWritePartitionDistributer(ISliceSerializer serializer, int instancesGroupingSize, int numSubgraphBins) {
		if (serializer == null) {
			throw new IllegalArgumentException();
		}
		if (instancesGroupingSize < 1) {
			throw new IllegalArgumentException();
		}
		if (numSubgraphBins < 1 && numSubgraphBins != -1) {
			throw new IllegalArgumentException();
		}

		_serializer = serializer;
		_instancesGroupingSize = instancesGroupingSize;
		_numSubgraphBins = numSubgraphBins;
	}

	@Override
	public UUID distribute(URI location, ISerializablePartition partition) throws IOException {
		Path path = Paths.get(location);
		
		// write slices for partition
		System.out.print("writing partition " + partition.getId() + " to " + location + "... ");
		long total = 0;

		int numSubgraphBins = _numSubgraphBins;
		if (_numSubgraphBins == -1 || _numSubgraphBins > partition.size()) {
			numSubgraphBins = partition.size();
		}

		ISliceManager sliceManager = SliceManager.create(_serializer, new FileStorageManager(path));
		total += sliceManager.writePartition(partition, _instancesGroupingSize, numSubgraphBins);

		System.out.println("[" + total / 1000 + " KB]");

		// append partition UUID information as fragment to location and return location
		return sliceManager.getPartitionUUID();
	}
}
