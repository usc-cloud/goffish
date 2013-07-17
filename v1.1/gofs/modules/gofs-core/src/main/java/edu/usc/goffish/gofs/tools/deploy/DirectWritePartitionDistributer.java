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

package edu.usc.goffish.gofs.tools.deploy;

import java.io.*;
import java.net.*;
import java.nio.file.*;

import edu.usc.goffish.gofs.*;
import edu.usc.goffish.gofs.slice.*;

public class DirectWritePartitionDistributer implements IPartitionDistributer {
	
	private final ISliceSerializer _serializer;
	
	public DirectWritePartitionDistributer() {
		this(new JavaSliceSerializer());
	}
	
	public DirectWritePartitionDistributer(ISliceSerializer serializer) {
		if (serializer == null) {
			throw new IllegalArgumentException();
		}
		
		_serializer = serializer;
	}
	
	@Override
	public URI distribute(URI location, ISerializablePartition partition) throws IOException {		
		// write slices for partition
		System.out.print("writing partition " + partition.getId() + " to " + location + "... ");
		long total = 0;
		
		SliceManager sliceManager = new SliceManager(_serializer, new FileStorageManager(Paths.get(location)));
		total += sliceManager.writePartition(partition);
		
		System.out.println("[" + total/1000 + " KB]");

		// append partition UUID information as fragment to location and return location
		return location.resolve("#" + sliceManager.getPartitionUUID());
	}
}
