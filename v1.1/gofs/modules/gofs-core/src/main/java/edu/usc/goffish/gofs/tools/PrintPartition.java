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

package edu.usc.goffish.gofs.tools;

import java.io.*;
import java.net.*;
import java.nio.file.*;
import java.util.*;

import edu.usc.goffish.gofs.*;
import edu.usc.goffish.gofs.slice.*;
import edu.usc.goffish.gofs.util.*;

public final class PrintPartition {

	private PrintPartition() {
		throw new UnsupportedOperationException();
	}

	public static void main(String[] args) throws IOException {
		if (args.length < REQUIRED_ARGS) {
			PrintUsageAndQuit(null);
		}

		ISliceSerializer serializer = new JavaSliceSerializer();
		
		// parse optional arguments
		int i;
		OptArgLoop:
		for (i = 0; i < args.length - REQUIRED_ARGS; i++) {
			switch (args[i]) {
			case "-serializer":
				i++;
				
				switch (args[i]) {
				case "java":
					break;
				case "kryo":
					serializer = new KryoSliceSerializer();
					break;
				default:
					PrintUsageAndQuit(null);
				}
				
				break;
			default:
				break OptArgLoop;
			}
		}
		
		while (i < args.length) {
			
			// required arguments
			Path partitionDirPath = null;
			UUID partitionUUID = null;
	
			// parse required arguments
			
			try {
				URI location = new URI(args[i]);
	
				try {
					if (location.getFragment() == null) {
						PrintUsageAndQuit("uri must have fragment for partition slice");
					}
					partitionUUID = UUID.fromString(location.getFragment());
				} catch (IllegalArgumentException e) {
					PrintUsageAndQuit("uri fragment must be a valid uuid - " + e.getMessage());
				}
	
				try {
					partitionDirPath = Paths.get(new URI(location.getScheme(), location.getSchemeSpecificPart(), null));
				} catch (URISyntaxException e) {
					PrintUsageAndQuit("problem removing fragment from partition uri - " + e.getMessage());
				} catch (IllegalArgumentException e) {
					PrintUsageAndQuit("partition uri must resolve to a valid path - " + e.getMessage());
				}
			} catch (URISyntaxException e) {
				PrintUsageAndQuit("partition location must be a valid uri - " + e.getMessage());
			}
	
			SliceManager sliceManager = new SliceManager(partitionUUID, serializer, new FileStorageManager(partitionDirPath));
	
			System.out.println("-----------------");
			System.out.print("Reading template... ");
	
			long time = System.currentTimeMillis();
			IPartition partition = sliceManager.readPartition();
	
			System.out.println("[" + (System.currentTimeMillis() - time) + "ms]");
	
			int numInstances = 0;
	
			System.out.println("Reading instances... ");
	
			time = System.currentTimeMillis();
			for (ISubgraph subgraph : partition) {
				System.out.println("Reading instances for subgraph " + subgraph.getId());
				Iterable<? extends ISubgraphInstance> instances = subgraph.getInstances(Long.MIN_VALUE, Long.MAX_VALUE, subgraph.getVertexProperties(), subgraph.getEdgeProperties(), false);
				numInstances += IterableHelper.iterableCount(instances);
			}
	
			System.out.println("Finished reading instances [" + (System.currentTimeMillis() - time) + "ms]");
	
			System.out.println("-----------------");
	
			System.out.println("Partition ID: " + partition.getId());
			System.out.println("Partition IsDirected: " + partition.isDirected());
			System.out.println("Partition Subgraphs: " + partition.size());
			System.out.println("Partition Instances: " + numInstances);
	
			int v = 0;
			long e = 0;
			int r = 0;
			for (ISubgraph subgraph : partition) {
				v += subgraph.getTemplate().numVertices();
				e += subgraph.getTemplate().numEdges();
				r += subgraph.getRemoteVertexMappings().size();
			}
	
			System.out.println("Partition Vertices: " + v);
			System.out.println("Partition Edges: " + e);
			System.out.println("Partition Remote Vertices: " + r);
			
			i++;
		}
	}

	private static final int REQUIRED_ARGS = 1;

	public static void PrintUsageAndQuit(String error) {
		if (error != null) {
			System.out.println("Error: " + error);
		}

		System.out.println("Usage:");
		System.out.println("  PrintPartition [args] <partitionuri1> [<partitionuri2> ...]");
		System.out.println("  Args: [-serializer java|kryo]");
		System.exit(0);
	}
}
