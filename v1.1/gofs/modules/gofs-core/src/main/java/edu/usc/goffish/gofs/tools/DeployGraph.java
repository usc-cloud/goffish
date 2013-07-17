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
import java.util.regex.*;

import edu.usc.goffish.gofs.*;
import edu.usc.goffish.gofs.formats.metis.*;
import edu.usc.goffish.gofs.graph.*;
import edu.usc.goffish.gofs.namenode.*;
import edu.usc.goffish.gofs.slice.*;
import edu.usc.goffish.gofs.tools.deploy.*;
import edu.usc.goffish.gofs.util.partitioning.*;
import edu.usc.goffish.gofs.util.partitioning.metis.*;
import edu.usc.goffish.gofs.util.partitioning.streaming.*;

public final class DeployGraph {

	private enum PartitionerMode {
		METIS, STREAM, PREDEFINED
	};

	private enum MapperMode {
		ROUNDROBIN
	};

	private enum DistributerMode {
		WRITE, SCP
	};

	private enum SerializationMode {
		JAVA, KRYO
	};

	private DeployGraph() {
		throw new UnsupportedOperationException();
	}

	private static void deploy(INameNode nameNode, String graphId, int numPartitions, IGraphLoader loader, IPartitioner partitioner, IPartitionBuilder partitionBuilder, IPartitionMapper partitionMapper, IPartitionDistributer partitionDistributer) throws IOException {
		if (nameNode == null) {
			throw new IllegalArgumentException();
		}
		if (graphId == null) {
			throw new IllegalArgumentException();
		}
		if (partitioner == null) {
			throw new IllegalArgumentException();
		}
		if (partitionMapper == null) {
			throw new IllegalArgumentException();
		}
		if (partitionDistributer == null) {
			throw new IllegalArgumentException();
		}

		// load template graph
		System.out.println("loading graph to partition... ");
		long time = System.currentTimeMillis();
		IIdentifiableVertexGraph<? extends IIdentifiableVertex, ? extends IEdge> graph = loader.loadGraph();
		System.out.println("loading finished [" + (System.currentTimeMillis() - time) + "ms]");
		
		// partition template
		System.out.println("partitioning graph...");
		time = System.currentTimeMillis();
		IPartitioning partitioning = partitioner.partition(graph, numPartitions);
		System.out.println("partitioning finished [" + (System.currentTimeMillis() - time) + "ms]");

		// taking too long during large deployments
		//long edgeCuts = PartitioningHelper.calculateEdgeCuts(partitioning, graph);
		//System.out.println("partitioning results: " + edgeCuts + " edge cuts (" + (int)(100*edgeCuts/(double)graph.numEdges()) + "%)");
		
		// release graph for gc
		graph = null;
		
		// partition template and instances
		System.out.println("building partitions...");
		time = System.currentTimeMillis();
		partitionBuilder.buildPartitions(partitioning);
		System.out.println("building finished [" + (System.currentTimeMillis() - time) + "ms]");
		
		// release partitioning for gc
		partitioning = null;
		
		int totalSubgraphs = 0;
		
		// write slices and deploy
		for (ISerializablePartition partition : partitionBuilder.getPartitions()) {
			int remoteVertices = partition.numRemoteVertices();
			int nonRemoteVertices = partition.numVertices() - remoteVertices;
			System.out.println("**partition " + partition.getId() + " has " + partition.size() + " subgraphs across " + nonRemoteVertices + " vertices and " + remoteVertices + " remote vertices");
			totalSubgraphs += partition.size();
			
			System.out.println("distributing partition " + partition.getId() + "...");
			time = System.currentTimeMillis();
			URI desiredLocation = partitionMapper.getLocationForPartition(partition);
			URI actualLocation = partitionDistributer.distribute(desiredLocation, partition);
			nameNode.putPartitionMapping(graphId, partition.getId(), actualLocation);
			System.out.println("distribution finished [" + (System.currentTimeMillis() - time) + "ms]");
		}
		
		System.out.println("**total subgraphs: " + totalSubgraphs);
	}

	public static void main(String[] args) throws IOException {
		if (args.length < REQUIRED_ARGS) {
			PrintUsageAndQuit(null);
		}
		
		// optional arguments
		PartitionerMode partitionerMode = PartitionerMode.METIS;
		MapperMode mapperMode = MapperMode.ROUNDROBIN;
		DistributerMode distributerMode = DistributerMode.WRITE;
		SerializationMode serializationMode = SerializationMode.JAVA;
		
		// optional sub arguments
		Path metisBinaryPath = null;
		String[] extraMetisOptions = null;
		Path partitioningPath = null;

		// parse optional arguments
		int i;
		OptArgLoop:
		for (i = 0; i < args.length - REQUIRED_ARGS; i++) {
			
			switch (args[i]) {
			case "-partitioner":
				i++;
				
				if (args[i].equals("stream")) {
					partitionerMode = PartitionerMode.STREAM;
				} else if (args[i].startsWith("metis")) {
					String[] subargs = parseSubArgs('=', args[i]);
					if (subargs[0].equals("metis")) {
						partitionerMode = PartitionerMode.METIS;
						if (subargs.length > 1) {
							try {
								metisBinaryPath = Paths.get(subargs[1]);
								if (!metisBinaryPath.isAbsolute()) {
									throw new InvalidPathException(metisBinaryPath.toString(), "metis binary path must be absolute");
								}
							} catch (InvalidPathException e) {
								PrintUsageAndQuit("metis binary - " + e.getMessage());
							}
							
							if (subargs.length > 2) {
								extraMetisOptions = parseSubArgs(' ', subargs[2]);
							}
						}
					} else {
						PrintUsageAndQuit(null);
					}
				} else if (args[i].startsWith("predefined")) {
					String[] subargs = parseSubArgs('=', args[i]);
					if (subargs[0].equals("predefined")) {
						partitionerMode = PartitionerMode.PREDEFINED;
						if (subargs.length < 2) {
							PrintUsageAndQuit(null);
						}
						
						try {
							partitioningPath = Paths.get(subargs[1]);
						} catch (InvalidPathException e) {
							PrintUsageAndQuit("partitioning file - " + e.getMessage());
						}
					} else {
						PrintUsageAndQuit(null);
					}
				} else {
					PrintUsageAndQuit(null);
				}
				
				break;
			case "-mapper":
				i++;
				
				if (args[i].equals("roundrobin")) {
					mapperMode = MapperMode.ROUNDROBIN;
				} else {
					PrintUsageAndQuit(null);
				}
				
				break;
			case "-distributer":
				i++;
				
				if (args[i].equals("write")) {
					distributerMode = DistributerMode.WRITE;
				} else if (args[i].equals("scp")) {
					distributerMode = DistributerMode.SCP;
				} else {
					PrintUsageAndQuit(null);
				}
				
				break;
			case "-serializer":
				i++;
				
				if (args[i].equals("java")) {
					serializationMode = SerializationMode.JAVA;
				} else if (args[i].equals("kryo")) {
					serializationMode = SerializationMode.KRYO;
				} else {
					PrintUsageAndQuit(null);
				}
				
				break;
			default:
				break OptArgLoop;
			}
		}

		if (args.length - i < REQUIRED_ARGS) {
			PrintUsageAndQuit(null);
		}
		
		// required arguments
		INameNode nameNode = null;
		String graphId = null;
		int numPartitions = 0;
		Path gmlTemplatePath = null;
		List<Path> gmlInstancePaths = new LinkedList<>();
		List<URI> locationURIs = new LinkedList<>();
		
		// parse required arguments
		
		try {
			if (args[i].startsWith("nnf")) {
				String[] subargs = parseSubArgs('=', args[i]);
				if (subargs[0].equals("nnf")) {
					if (subargs.length < 2) {
						PrintUsageAndQuit(null);
					}
					nameNode = new FileNameNode(Paths.get(subargs[1]));
				} else {
					PrintUsageAndQuit(null);
				}
			} else if (args[i].equals("nns")) {
				String[] subargs = parseSubArgs('=', args[i]);
				if (subargs[0].equals("nns")) {
					if (subargs.length < 2) {
						PrintUsageAndQuit(null);
					}
					
					String[] info = subargs[1].split(":");
					if (info.length < 2) {
						PrintUsageAndQuit(null);
					}
				
					nameNode = new RemoteNameNode(info[0], Integer.parseInt(info[1]));
				} else {
					PrintUsageAndQuit(null);
				}
			} else {
				PrintUsageAndQuit(null);
			}
		} catch (InvalidPathException e) {
			PrintUsageAndQuit("name node file - " + e.getMessage());
		} catch (NumberFormatException e) {
			PrintUsageAndQuit("name node server port - " + e.getMessage());
		}

		graphId = args[i + 1];
		
		try {
			numPartitions = Integer.parseInt(args[i + 2]);
		} catch (NumberFormatException e) {
			PrintUsageAndQuit("number of partitions - " + e.getMessage());
		}

		try {
			Path gmlInputFile = Paths.get(args[i + 3]);
			try (BufferedReader input = new BufferedReader(new InputStreamReader(Files.newInputStream(gmlInputFile)))) {
				String line = input.readLine();
				gmlTemplatePath = Paths.get(line);

				line = input.readLine();
				while (line != null) {
					gmlInstancePaths.add(Paths.get(line));
					line = input.readLine();
				}
			} catch (InvalidPathException e) {
				PrintUsageAndQuit("gml input file - " + e.getMessage());
			}
		} catch (InvalidPathException e) {
			PrintUsageAndQuit(e.getMessage());
		}

		try {
			Path locationInputFile = Paths.get(args[i + 4]);
			try (BufferedReader input = new BufferedReader(new InputStreamReader(Files.newInputStream(locationInputFile)))) {
				String line = input.readLine();
				while (line != null) {
					locationURIs.add(new URI(line));
					line = input.readLine();
				}
			} catch (URISyntaxException e) {
				PrintUsageAndQuit("location input file - " + e.getMessage());
			}
		} catch (InvalidPathException e) {
			PrintUsageAndQuit(e.getMessage());
		}
		
		// create loader
		IGraphLoader loader = new GMLGraphLoader(gmlTemplatePath);
		
		// create partitioner
		IPartitioner partitioner = null;
		switch (partitionerMode) {
		case METIS:
			if (metisBinaryPath == null) {
				partitioner = new MetisPartitioner();
			} else {
				partitioner = new MetisPartitioner(metisBinaryPath, extraMetisOptions);
			}
			break;
		case STREAM:
			partitioner = new StreamPartitioner(new LDGObjectiveFunction());
			break;
		case PREDEFINED:
			partitioner = new PredefinedPartitioner(MetisPartitioning.read(Files.newInputStream(partitioningPath)));
			break;
		default:
			PrintUsageAndQuit(null);
		}
		
		// create mapper
		IPartitionMapper partitionMapper = null;
		switch (mapperMode) {
		case ROUNDROBIN:
			partitionMapper = new RoundRobinPartitionMapper(locationURIs);
			break;
		default:
			PrintUsageAndQuit(null);
		}
		
		// create serializer
		ISliceSerializer serializer = null;
		switch (serializationMode) {
		case JAVA:
			serializer = new JavaSliceSerializer();
			break;
		case KRYO:
			serializer = new KryoSliceSerializer();
			break;
		default:
			PrintUsageAndQuit(null);
		}
		
		// create distributer
		IPartitionDistributer partitionDistributer = null;
		switch (distributerMode) {
		case WRITE:
			partitionDistributer = new DirectWritePartitionDistributer(serializer);
			break;
		case SCP:
			partitionDistributer = new SCPPartitionDistributer(serializer);
			break;
		default:
			PrintUsageAndQuit(null);
		}

		
		// create builder
		try (GMLPartitionBuilder partitionBuilder = new GMLPartitionBuilder(gmlTemplatePath, gmlInstancePaths)) {
			
			// perform deployment
			long time = System.currentTimeMillis();
			deploy(nameNode, graphId, numPartitions, loader, partitioner, partitionBuilder, partitionMapper, partitionDistributer);
			System.out.println("finished [total " + (System.currentTimeMillis() - time) + "ms]");
		}
	}
	
	private static String[] parseSubArgs(char split, String arg) {
		List<String> matchList = new LinkedList<String>();
		Pattern regex = Pattern.compile("[^" + split + "\"]+|\"([^\"]*)\"");
		Matcher regexMatcher = regex.matcher(arg);
		while (regexMatcher.find()) {
		    if (regexMatcher.group(1) != null) {
		        matchList.add(regexMatcher.group(1));
		    } else {
		        matchList.add(regexMatcher.group());
		    }
		}
		
		return matchList.toArray(new String[matchList.size()]);
	}
	
	private static final int REQUIRED_ARGS = 5;

	private static void PrintUsageAndQuit(String error) {
		if (error != null) {
			System.out.println("Error: " + error);
		}

		System.out.println("Usage:");
		System.out.println("  DeployGraph [args] nnf=<namenodefile>|nns=<host>:<port> <graphid> <numpartitions> <gmlinputfile> <locationinputfile>");
		System.out.println("  Args: [-partitioner stream|metis[=<pathtometisbinary>[=<metisargs>]]|predefined=<partitioningfile>]");
		System.out.println("        [-mapper roundrobin]");
		System.out.println("        [-distributer write|scp]");
		System.out.println("        [-serializer java|kryo]");
		System.out.println();
		System.out.println("    Options:");
		System.out.println("      The -partitioner flag selects the partioning to use. Selecting metis allows for an optional argument with");
		System.out.println("    the path to the metis binary. If not specified, the binary is assumed to be 'gpmetis' and available on the");
		System.out.println("    PATH. Selecting predefined allows for a required argument, the path to a METIS style partitioning file.");
		System.out.println("    distributed to each uri location. If predefined is selected, the value supplied for <numpartitions> will");
		System.out.println("    be ignored. If the -partitioner flag is not specified, METIS is used.");
		System.out.println("      The -mapper flag selects the mapper to use to map partitions to final locations as found in the location");
		System.out.println("    input file. If the -mapper flag is not specified, round robin mapping is used.");
		System.out.println("      The -distributer flag selects the method of distributing partitions to their final destination. Selecting");
		System.out.println("    write attempts to write files directly to their location. The location must be accessible through the file");
		System.out.println("    system, whether local, or a network share, etc... Selecting scp uses SCP to transfer the files to their");
		System.out.println("    location. Passwordless SCP must be enabled to the destination. Host, path, and SCP port will be grabbed");
		System.out.println("    from the location URIs. If the -distributer flag is not specified, direct write is used.");
		System.out.println("      The -serializer flag selects the serialization format for writing slices to disk. If the -serializer");
		System.out.println("    flag is not specified, java serialization is used.");
		System.out.println("      The name node is specified either as a file or server, using nnf: and nns: respectively. If specified as a");
		System.out.println("    file, <namenodefile> is the path to the file. If as a server, <host> and <port> specify the host and port the");
		System.out.println("    name node server is operating on.");
		System.out.println("      The <gmlinputfile> is a text file containing a list of GML files, one per line. The first line must be the");
		System.out.println("    path to the template GML file, every following line is the path to an instance GML file.");
		System.out.println("      The <locationinputfile> is a text file containing a list of valid URIs, one per line. Each URI specifies a");
		System.out.println("    location to distribute partitions to, and is interpreted by the distributer (i.e. the SCP distributer pulls");
		System.out.println("    out a specfied port and uses it as the SCP port).");
		System.exit(1);
	}
}
