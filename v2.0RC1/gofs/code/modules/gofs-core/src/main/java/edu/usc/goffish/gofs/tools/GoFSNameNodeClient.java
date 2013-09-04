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

import it.unimi.dsi.fastutil.ints.*;

import java.io.*;
import java.net.*;
import java.util.*;

import edu.usc.goffish.gofs.namenode.*;
import edu.usc.goffish.gofs.partition.*;
import edu.usc.goffish.gofs.slice.*;

public class GoFSNameNodeClient {

	private GoFSNameNodeClient() {
		throw new UnsupportedOperationException();
	}

	private static void handleDataNodes(IInternalNameNode nameNode, String[] args) throws IOException {
		switch (args[2]) {
		case "get":
		{
			Set<URI> dataNodes = nameNode.getDataNodes();
			for (URI dataNode : dataNodes) {
				System.out.println(dataNode);
			}
		}
			break;
		case "add":
		{
			if (args.length < 4) {
				PrintUsageAndQuit(null);
			}

			URI dataNodeURI = null;
			try {
				dataNodeURI = new URI(args[3]);
			} catch (URISyntaxException e) {
				PrintUsageAndQuit("datanodes add uri - " + e.getMessage());
			}

			nameNode.addDataNode(dataNodeURI);
			System.out.println("Name node added data node " + dataNodeURI);
		}
			break;
		default:
			PrintUsageAndQuit(null);
		}
	}

	private static void handleSerializer(IInternalNameNode nameNode, String[] args) throws IOException {
		switch (args[2]) {
		case "get":
		{
			ISliceSerializer serializer = nameNode.getSerializer();
			System.out.println(serializer.getClass().getName());
		}
			break;
		case "set":
		{
			if (args.length < 4) {
				PrintUsageAndQuit(null);
			}

			Class<? extends ISliceSerializer> serializerType = null;
			try {
				serializerType = SliceSerializerProvider.loadSliceSerializerType(args[3]);
			} catch (ReflectiveOperationException e) {
				PrintUsageAndQuit("serializer set classtype - " + e.getMessage());
			}

			nameNode.setSerializer(serializerType);
			System.out.println("Name node serializer type set to " + serializerType.getName());
		}
			break;
		default:
			PrintUsageAndQuit(null);
		}
	}

	private static void handleDirectory(IInternalNameNode nameNode, String[] args) throws IOException {
		switch (args[2]) {
		case "get":
		{
			Collection<String> graphs = nameNode.getPartitionDirectory().getGraphs();
			for (String graphId : graphs) {
				IntCollection partitions = nameNode.getPartitionDirectory().getPartitions(graphId);
				for (int partitionId : partitions) {
					System.out.println(graphId + " -> " + partitionId + " -> " + nameNode.getPartitionDirectory().getPartitionMapping(graphId, partitionId));
				}
			}
		}
			break;
		case "remove":
		{
			if (args.length < 5) {
				PrintUsageAndQuit(null);
			}

			String graphId = args[3];
			int partitionId = Partition.INVALID_PARTITION;
			try {
				partitionId = Integer.parseInt(args[4]);
			} catch (NumberFormatException e) {
				PrintUsageAndQuit("directory remove partitionid - " + e.getMessage());
			}

			nameNode.getPartitionDirectory().removePartitionMapping(graphId, partitionId);
			System.out.println("Directory mapping for graph " + graphId + ", partition id " + partitionId + " removed");
		}
			break;
		case "put":
		{
			if (args.length < 6) {
				PrintUsageAndQuit(null);
			}

			String graphId = args[3];
			int partitionId = Partition.INVALID_PARTITION;
			try {
				partitionId = Integer.parseInt(args[4]);
			} catch (NumberFormatException e) {
				PrintUsageAndQuit("directory put partitionid - " + e.getMessage());
			}
			URI dataNodeURI = null;
			try {
				dataNodeURI = new URI(args[5]);
			} catch (URISyntaxException e) {
				PrintUsageAndQuit("directory put datanodeuri - " + e.getMessage());
			}

			nameNode.getPartitionDirectory().putPartitionMapping(graphId, partitionId, dataNodeURI);
			System.out.println("Directory mapping for graph " + graphId + ", partition id " + partitionId + ", data node " + dataNodeURI + " set");
		}
			break;
		default:
			PrintUsageAndQuit(null);
		}
	}

	public static void main(String[] args) throws IOException {
		if (args.length < REQUIRED_ARGS) {
			PrintUsageAndQuit(null);
		}

		if (args.length == 1 && args[0].equals("-help")) {
			PrintUsageAndQuit(null);
		}

		URI nameNodeURI = null;
		try {
			nameNodeURI = new URI(args[0]);
		} catch (URISyntaxException e) {
			PrintUsageAndQuit("name node uri - " + e.getMessage());
		}

		IInternalNameNode nameNode = new RemoteNameNode(nameNodeURI);
		if (!nameNode.isAvailable()) {
			throw new IOException("Name node at " + nameNode.getURI() + " is not available");
		}

		switch (args[1]) {
		case "datanodes":
			handleDataNodes(nameNode, args);
			break;
		case "serializer":
			handleSerializer(nameNode, args);
			break;
		case "directory":
			handleDirectory(nameNode, args);
			break;
		default:
			PrintUsageAndQuit(null);
		}
	}

	private static final int REQUIRED_ARGS = 3;

	private static void PrintUsageAndQuit(String error) {
		if (error != null) {
			System.out.println("Error: " + error);
		}

		System.out.println("Usage:");
		System.out.println("  GoFSNameNodeClient -help");
		System.out.println("      Displays this help message.");
		System.out.println("  GoFSNameNodeClient <namenodeuri> <command> <args>");
		System.out.println("  Commands <Args>:");
		System.out.println("    datanodes get");
		System.out.println("    datanodes clear");
		System.out.println("    datanodes add <datanodeuri>");
		System.out.println("    serializer get");
		System.out.println("    serializer set <serializerclass>");
		System.out.println("    directory get");
		System.out.println("    directory remove <graphid> <partitionid>");
		System.out.println("    directory put <graphid> <partitionid> <datanodeuri>");
		System.out.println();
		System.out.println("      This program allows for interaction with a running name node server and");
		System.out.println("    both viewing and changing its state in a variety of ways. This program is");
		System.out.println("    meant for debugging purposes only, as it is extremely easy to corrupt a");
		System.out.println("    name node completely. There is no undo button, so be careful with your");
		System.out.println("    changes!");

		System.exit(1);
	}
}
