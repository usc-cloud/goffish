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
import java.util.*;

import edu.usc.goffish.gofs.*;
import edu.usc.goffish.gofs.slice.*;
import edu.usc.goffish.gofs.util.*;

public class SCPPartitionDistributer implements IPartitionDistributer {
	
	private static final String DefaultSCPBinary = "scp";

	private final Path _scpBinaryPath;
	private final String[] _extraSCPOptions;
	private final ISliceSerializer _serializer;
	
	public SCPPartitionDistributer() {
		this(Paths.get(DefaultSCPBinary), null);
	}
	
	public SCPPartitionDistributer(ISliceSerializer serializer) {
		this(Paths.get(DefaultSCPBinary), null, serializer);
	}
	
	public SCPPartitionDistributer(Path scpBinaryPath, String[] extraSCPOptions) {
		this(scpBinaryPath, extraSCPOptions, new JavaSliceSerializer());
	}
	
	public SCPPartitionDistributer(Path scpBinaryPath, String[] extraSCPOptions, ISliceSerializer serializer) {
		if (scpBinaryPath == null) {
			throw new IllegalArgumentException();
		}
		if (serializer == null) {
			throw new IllegalArgumentException();
		}
		
		_scpBinaryPath = scpBinaryPath.normalize();
		_extraSCPOptions = extraSCPOptions;
		_serializer = serializer;
	}
	
	@Override
	public URI distribute(URI location, ISerializablePartition partition) throws IOException {
		Path workingDir = Files.createTempDirectory("gofs_scpdist");

		System.out.print("writing partition... ");
		long total = 0;
		
		// write slices for partition
		SliceManager sliceManager = new SliceManager(_serializer, new FileStorageManager(workingDir));
		total += sliceManager.writePartition(partition);
	
		System.out.println("[" + total/1000 + " KB]");
		
		// prepare list of files to scp
		List<Path> sliceFiles = new LinkedList<>();
		try (DirectoryStream<Path> sliceDir = Files.newDirectoryStream(workingDir)) {
			for (Path slice : sliceDir) {
				sliceFiles.add(slice);
			}
		}

		// scp files
		System.out.println("moving partition " + partition.getId() + " to " + location + "...");
		
		SCPHelper.SCP(_scpBinaryPath, _extraSCPOptions, sliceFiles, location);
		
		// delete files
		FileHelper.delete(workingDir.toFile());

		// append partition UUID information as fragment to location and return location
		return location.resolve("#" + sliceManager.getPartitionUUID());
	}
}
