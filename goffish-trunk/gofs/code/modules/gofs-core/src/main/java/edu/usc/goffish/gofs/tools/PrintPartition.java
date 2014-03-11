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

package edu.usc.goffish.gofs.tools;

import java.io.*;
import java.net.*;
import java.nio.file.*;
import java.util.*;

import edu.usc.goffish.gofs.*;
import edu.usc.goffish.gofs.namenode.IInternalNameNode;
import edu.usc.goffish.gofs.namenode.NameNodeProvider;
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

		// optional args
		boolean readInstances = false;
		String namenodetype = null;
		URI namenodeuri = null;
		String graphid = null;
		int partitionId = -1;
		// parse optional arguments
		int i;
		OptArgLoop: for (i = 0; i < args.length; i++) {
			switch (args[i]) {
			case "-namenodetype":
				namenodetype = args[++i];
				break;
			case "-namenodeuri":
				namenodeuri = URI.create(args[++i]);
				break;
			case "-graphid":
			    graphid = args[++i];
			    break;
			case "-partitionid":
				partitionId = Integer.parseInt(args[++i]);
				break;
			case "-instances":
				readInstances = true;
				break;
			default:
				break OptArgLoop;
			}
		}
		
		// required arguments
		Path partitionDirPath = null;
		UUID partitionUUID = null;

		IInternalNameNode namenode = loadNameNode(namenodetype, namenodeuri);
		URI location = namenode.getPartitionDirectory().getPartitionMapping(graphid, partitionId);

		try {
			if (location.getFragment() == null) {
				PrintUsageAndQuit("uri must have fragment for partition slice");
			}
			partitionUUID = UUID.fromString(location.getFragment());
		} catch (IllegalArgumentException e) {
			PrintUsageAndQuit("uri fragment must be a valid uuid - " + e.getMessage());
		}

		try {
			partitionDirPath = Paths.get(new URI(location.getScheme(), null, location.getPath(), null)).resolve("slices");
		} catch (URISyntaxException e) {
			PrintUsageAndQuit("problem removing fragment from partition uri - " + e.getMessage());
		} catch (IllegalArgumentException e) {
			PrintUsageAndQuit("partition uri must resolve to a valid path - " + e.getMessage());
		}

		Runtime runtime = Runtime.getRuntime();

		SliceManager sliceManager = (SliceManager)SliceManager.create(partitionUUID, namenode.getSerializer(), new FileStorageManager(partitionDirPath));

		System.out.print("Reading template... ");

		runtime.gc();
		long memory = (runtime.totalMemory() - runtime.freeMemory());

		long time = System.currentTimeMillis();
		IPartition partition = sliceManager.readPartition();

		runtime.gc();
		System.out.print("[" + ((runtime.totalMemory() - runtime.freeMemory()) - memory) / 1000 + " KB in memory] ");
		System.out.println("[" + (System.currentTimeMillis() - time) + "ms]");

		TreeMap<Integer, Integer> subgraphVertexHistogram = new TreeMap<>();

		long instanceTime = 0;
		if (readInstances) {
			System.out.print("Reading instances... ");
		}

		int numInstances = 0;
		for (ISubgraph subgraph : partition) {
			Iterable<? extends ISubgraphInstance> instances = subgraph.getInstances(Long.MIN_VALUE, Long.MAX_VALUE, subgraph.getVertexProperties(), subgraph.getEdgeProperties(), false);
			numInstances += IterableUtils.iterableCount(instances);

			if (readInstances) {
				time = System.currentTimeMillis();
				for (ISubgraphInstance instance : instances) {
					instance.getPropertiesForEdge(1);
				}
				instanceTime += (System.currentTimeMillis() - time);
			}

			Integer j = subgraphVertexHistogram.get(subgraph.numVertices());
			if (j == null) {
				j = new Integer(0);
			}
			subgraphVertexHistogram.put(subgraph.numVertices(), j + 1);
		}

		if (readInstances) {
			System.out.println("[" + instanceTime + "ms]");
		}

		System.out.println("-----------------");

		System.out.println("Partition Metadata UUID: " + sliceManager.getPartitionUUID());
		System.out.println("Partition Template UUID: " + sliceManager.getPartitionTemplateUUID());
		System.out.println("Partition ID: " + partition.getId());
		System.out.println("Partition IsDirected: " + partition.isDirected());
		System.out.println("Partition Subgraphs: " + partition.size());
		System.out.println("Partition Instances: " + numInstances);

		int v = 0;
		long e = 0;
		int r = 0;
		for (ISubgraph subgraph : partition) {
			v += subgraph.numVertices();
			e += subgraph.numEdges();
			r += subgraph.numRemoteVertices();
		}

		System.out.println("Partition Vertices: " + v);
		System.out.println("Partition Edges: " + e);
		System.out.println("Partition Remote Vertices: " + r);

		System.out.println("-----------------");

		if (readInstances) {
			System.out.println("Linear Access Cache Hits: " + sliceManager.getCacheHits() + "/" + sliceManager.getCacheQueries());
		}

		System.out.println("Partition Subgraph Histogram:");
		System.out.format("%1$8s : %2$8s\n", "Vertices", "Count");
		for (Map.Entry<Integer, Integer> entry : subgraphVertexHistogram.entrySet()) {
			System.out.format("%1$8d : %2$8d\n", entry.getKey(), entry.getValue());
		}
	}
	
	private static IInternalNameNode loadNameNode(String nameNodeType, URI nameNodeUri)
			throws IOException {
		IInternalNameNode nameNode;
		try {
			nameNode = NameNodeProvider.loadNameNode(nameNodeType, nameNodeUri);
		} catch (ReflectiveOperationException e) {
			throw new RuntimeException("Unable to load name node", e);
		}

		System.out.println("Contacting name node for " + nameNodeUri + " uri..");

		// validate name node
		if (!nameNode.isAvailable()) {
			throw new IOException("Name node at " + nameNode.getURI() + " is not available");
		}

		return nameNode;
	}


	private static final int REQUIRED_ARGS = 8;

	public static void PrintUsageAndQuit(String error) {
		if (error != null) {
			System.out.println("Error: " + error);
		}

		System.out.println("Usage:");
		System.out.println("  PrintPartition -namenodetype <namenodetype> -namenodeuri <namenodeuri> -graphid <graphid> -partitionid " +
				"<partitionid> -instances");

		System.exit(0);
	}
}
