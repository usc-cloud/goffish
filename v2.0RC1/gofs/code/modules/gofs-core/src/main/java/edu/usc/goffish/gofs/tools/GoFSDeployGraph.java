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
import it.unimi.dsi.fastutil.longs.*;

import java.io.*;
import java.net.*;
import java.nio.file.*;
import java.util.*;
import java.util.regex.*;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;

import edu.usc.goffish.gofs.*;
import edu.usc.goffish.gofs.formats.metis.*;
import edu.usc.goffish.gofs.graph.*;
import edu.usc.goffish.gofs.namenode.*;
import edu.usc.goffish.gofs.partition.*;
import edu.usc.goffish.gofs.partition.components.*;
import edu.usc.goffish.gofs.slice.*;
import edu.usc.goffish.gofs.tools.deploy.*;
import edu.usc.goffish.gofs.util.partitioning.*;
import edu.usc.goffish.gofs.util.partitioning.metis.*;
import edu.usc.goffish.gofs.util.partitioning.streaming.*;

public final class GoFSDeployGraph {

	private enum DistributerMode {
		SCP, WRITE
	};

	private enum PartitionerMode {
		METIS, STREAM, PREDEFINED
	};

	private enum ComponentizerMode {
		SINGLE, WCC
	};

	private enum MapperMode {
		ROUNDROBIN
	};

	private enum PartitionedFileMode{
		DEFAULT, SAVE, READ;
	}

	private GoFSDeployGraph() {
		throw new UnsupportedOperationException();
	}

	private static void deploy(IPartitionDirectory partitionDirectory, String graphId, int numPartitions,  
			IGraphLoader loader, IPartitioner partitioner, IPartitionBuilder partitionBuilder, String intermediateGMLInputFile, IPartitionMapper partitionMapper, IPartitionDistributer partitionDistributer) throws IOException {
		if (partitionDirectory == null) {
			throw new IllegalArgumentException();
		}
		if (graphId == null) {
			throw new IllegalArgumentException();
		}
		if (numPartitions < 1) {
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

		IPartitioning partitioning;
		if (numPartitions == 1) {
			partitioning = new SinglePartitioning(graph);
		} else {
			// partition template
			System.out.println("partitioning graph...");
			time = System.currentTimeMillis();
			partitioning = partitioner.partition(graph, numPartitions);
			System.out.println("partitioning finished [" + (System.currentTimeMillis() - time) + "ms]");
		}

		// release graph for gc
		graph = null;

		// partition template and instances
		System.out.println("building partitions...");
		time = System.currentTimeMillis();
		partitionBuilder.buildPartitions(partitioning);
		if(intermediateGMLInputFile != null){
			((GMLPartitionBuilder)partitionBuilder).new XMLConfigurationBuilder(intermediateGMLInputFile).saveIntermediateGMLFile();
		}
		System.out.println("building finished [" + (System.currentTimeMillis() - time) + "ms]");

		// release partitioning for gc
		partitioning = null;

		distributePartitions(partitionDirectory, graphId, partitionBuilder,
				partitionMapper, partitionDistributer);
	}

	private static void distributePartitions(IPartitionDirectory partitionDirectory, String graphId,
			IPartitionBuilder partitionBuilder,	IPartitionMapper partitionMapper,
			IPartitionDistributer partitionDistributer) throws IOException {
		//collect remote vertices
		LongArrayList remoteVertices = collectRemoteVertices(partitionBuilder);

		//map remote vertices
		Long2LongOpenHashMap remoteVerticesMappings = mapRemoteVertices(
				partitionBuilder, remoteVertices);

		// release for gc
		remoteVertices = null;

		//write slices
		writeSlices(partitionDirectory, graphId, partitionBuilder,
				partitionMapper, partitionDistributer, remoteVerticesMappings);
	}

	private static void writeSlices(IPartitionDirectory partitionDirectory,
			String graphId, IPartitionBuilder partitionBuilder, IPartitionMapper partitionMapper,
			IPartitionDistributer partitionDistributer, Long2LongOpenHashMap remoteVerticesMappings) throws IOException {
		long time;
		int totalSubgraphs = 0;

		// write slices and deploy
		for (ISerializablePartition partition : partitionBuilder.getPartitions()) {

			// assign remote vertex subgraph ids
			for (ISubgraph subgraph : partition) {
				for (ITemplateVertex remoteVertex : subgraph.remoteVertices()) {
					// CHEAT
					if (!(remoteVertex instanceof TemplateRemoteVertex)) {
						throw new IllegalStateException();
					}
					((TemplateRemoteVertex)remoteVertex).setRemoteSubgraphId(remoteVerticesMappings.get(remoteVertex.getId()));
				}
			}

			System.out.println("**partition " + partition.getId() + " has " + partition.size() + " subgraphs across " + partition.numVertices() + " vertices");
			totalSubgraphs += partition.size();

			System.out.println("distributing partition " + partition.getId() + "...");
			time = System.currentTimeMillis();

			URI dataNodeLocation = partitionMapper.getLocationForPartition(partition);
			URI slicesLocation = dataNodeLocation.resolve(DataNode.DATANODE_SLICE_DIR.toString());

			UUID fragment = partitionDistributer.distribute(slicesLocation, partition);
			partitionDirectory.putPartitionMapping(graphId, partition.getId(), dataNodeLocation.resolve("#" + fragment.toString()));

			System.out.println("distribution finished [" + (System.currentTimeMillis() - time) + "ms]");
		}

		System.out.println("**total subgraphs: " + totalSubgraphs);
	}

	private static Long2LongOpenHashMap mapRemoteVertices(
			IPartitionBuilder partitionBuilder, LongArrayList remoteVertices)
					throws IOException {
		// map remote vertices
		Long2LongOpenHashMap remoteVerticesMappings = new Long2LongOpenHashMap(remoteVertices.size(), 1f);
		for (ISerializablePartition partition : partitionBuilder.getPartitions()) {
			for (long remoteVertexId : remoteVertices) {
				ISubgraph subgraph = partition.getSubgraphForVertex(remoteVertexId);
				if (subgraph != null && !subgraph.getVertex(remoteVertexId).isRemote()) {
					assert (!remoteVerticesMappings.containsKey(remoteVertexId));
					remoteVerticesMappings.put(remoteVertexId, subgraph.getId());
				}
			}
		}
		return remoteVerticesMappings;
	}

	private static LongArrayList collectRemoteVertices(
			IPartitionBuilder partitionBuilder) throws IOException {
		// collect remote vertices
		LongArrayList remoteVertices = new LongArrayList();
		for (IPartition partition : partitionBuilder.getPartitions()) {
			for (ISubgraph subgraph : partition) {
				for (ITemplateVertex remoteVertex : subgraph.remoteVertices()) {
					remoteVertices.add(remoteVertex.getId());
				}
			}
		}
		return remoteVertices;
	}

	private static void deploy(IPartitionDirectory partitionDirectory, String graphId, int numPartitions, IPartitionBuilder partitionBuilder, IPartitionMapper partitionMapper, IPartitionDistributer partitionDistributer) throws IOException {
		if (partitionDirectory == null) {
			throw new IllegalArgumentException();
		}
		if (graphId == null) {
			throw new IllegalArgumentException();
		}
		if (numPartitions < 1) {
			throw new IllegalArgumentException();
		}
		if (partitionMapper == null) {
			throw new IllegalArgumentException();
		}
		if (partitionDistributer == null) {
			throw new IllegalArgumentException();
		}

		distributePartitions(partitionDirectory, graphId, partitionBuilder,
				partitionMapper, partitionDistributer);
	}

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws IOException {
		if (args.length < REQUIRED_ARGS) {
			PrintUsageAndQuit(null);
		}

		if (args.length == 1 && args[0].equals("-help")) {
			PrintUsageAndQuit(null);
		}

		// optional arguments
		boolean overwriteGraph = false;
		PartitionerMode partitionerMode = PartitionerMode.METIS;
		ComponentizerMode componentizerMode = ComponentizerMode.WCC;
		MapperMode mapperMode = MapperMode.ROUNDROBIN;
		PartitionedFileMode partitionedFileMode = PartitionedFileMode.DEFAULT;
		DistributerMode distributerMode = DistributerMode.SCP;
		int instancesGroupingSize = 1;
		int numSubgraphBins = -1;

		// optional sub arguments
		Path metisBinaryPath = null;
		String[] extraMetisOptions = null;
		Path partitioningPath = null;
		Path partitionedGMLFilePath = null;

		// parse optional arguments
		int i = 0;
		OptArgLoop: for (i = 0; i < args.length - REQUIRED_ARGS; i++) {

			switch (args[i]) {
			case "-overwriteGraph":
				overwriteGraph = true;
				break;
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
			case "-intermediategml":
				if(args[i+1].startsWith("save")){
					i++;
					String[] subargs = parseSubArgs('=', args[i]);
					if(subargs[0].equals("save")){
						if (subargs.length < 2) {
							PrintUsageAndQuit(null);
						}

						partitionedFileMode = PartitionedFileMode.SAVE;
						try {
							partitionedGMLFilePath = Paths.get(subargs[1]);
						} catch (InvalidPathException e) {
							PrintUsageAndQuit("partitioned gml file  - " + e.getMessage());
						}
					}
				}else{
					partitionedFileMode = PartitionedFileMode.READ;
				}
				break;
			case "-componentizer":
				i++;

				switch (args[i]) {
				case "single":
					componentizerMode = ComponentizerMode.SINGLE;
					break;
				case "wcc":
					componentizerMode = ComponentizerMode.WCC;
					break;
				default:
					PrintUsageAndQuit(null);
				}

				break;
			case "-distributer":
				i++;

				switch (args[i]) {
				case "scp":
					distributerMode = DistributerMode.SCP;
					break;
				case "write":
					distributerMode = DistributerMode.WRITE;
					break;
				default:
					PrintUsageAndQuit(null);
				}

				break;
			case "-mapper":
				i++;

				if (args[i].equalsIgnoreCase("roundrobin")) {
					mapperMode = MapperMode.ROUNDROBIN;
				} else {
					PrintUsageAndQuit(null);
				}

				break;
			case "-serializer:instancegroupingsize":
				i++;

				try {
					if (args[i].equalsIgnoreCase("ALL")) {
						instancesGroupingSize = Integer.MAX_VALUE;
					} else {
						instancesGroupingSize = Integer.parseInt(args[i]);
						if (instancesGroupingSize < 1) {
							PrintUsageAndQuit("Serialization instance grouping size must be greater than zero");
						}
					}
				} catch (NumberFormatException e) {
					PrintUsageAndQuit("Serialization instance grouping size - " + e.getMessage());
				}

				break;
			case "-serializer:numsubgraphbins":
				i++;

				try {
					numSubgraphBins = Integer.parseInt(args[i]);
					if (instancesGroupingSize < 1) {
						PrintUsageAndQuit("Serialization number of subgraph bins must be greater than zero");
					}
				} catch (NumberFormatException e) {
					PrintUsageAndQuit("Serialization number of subgraph bins - " + e.getMessage());
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
		IInternalNameNode nameNode = null;
		Class<? extends IInternalNameNode> nameNodeType = null;
		URI nameNodeLocation = null;
		String graphId = null;
		int numPartitions = 0;
		Path gmlTemplatePath = null;
		List<Path> gmlInstancePaths = new LinkedList<>();

		// parse required arguments

		try {
			nameNodeType = NameNodeProvider.loadNameNodeType(args[i]);
			i++;
		} catch (ReflectiveOperationException e) {
			PrintUsageAndQuit("name node type - " + e.getMessage());
		}

		try {
			nameNodeLocation = new URI(args[i]);
			i++;
		} catch (URISyntaxException e) {
			PrintUsageAndQuit("name node location - " + e.getMessage());
		}

		try {
			nameNode = NameNodeProvider.loadNameNode(nameNodeType, nameNodeLocation);
		} catch (ReflectiveOperationException e) {
			PrintUsageAndQuit("error loading name node - " + e.getMessage());
		}

		graphId = args[i++];

		try {
			numPartitions = Integer.parseInt(args[i]);
			i++;
		} catch (NumberFormatException e) {
			PrintUsageAndQuit("number of partitions - " + e.getMessage());
		}

		Path gmlInputFile = null;
		try {
			gmlInputFile = Paths.get(args[i]);
			i++;
		} catch (InvalidPathException e) {
			PrintUsageAndQuit(e.getMessage());
		}

		// finished parsing args
		if (i < args.length) {
			PrintUsageAndQuit("Unrecognized argument \"" + args[i] + "\"");
		}

		// ensure name node is available
		if (!nameNode.isAvailable()) {
			throw new IOException("Name node at " + nameNode.getURI() + " is not available");
		}

		// ensure there are data nodes available
		Set<URI> dataNodes = nameNode.getDataNodes();
		if (dataNodes == null || dataNodes.isEmpty()) {
			throw new IllegalArgumentException("name node does not have any data nodes available for deployment");
		}

		// ensure graph id does not exist (unless to be overwritten)
		IntCollection partitions = nameNode.getPartitionDirectory().getPartitions(graphId);
		if (partitions != null) {
			if (!overwriteGraph) {
				throw new IllegalArgumentException("graph id \"" + graphId + "\" already exists in name node partition directory");
			} else {
				for (int partitionId : partitions) {
					nameNode.getPartitionDirectory().removePartitionMapping(graphId, partitionId);
				}
			}
		}

		IGraphLoader loader = null;
		IPartitioner partitioner = null;
		if(partitionedFileMode != PartitionedFileMode.READ){
			XMLConfiguration configuration;
			try {
				configuration = new XMLConfiguration(gmlInputFile.toFile());
				configuration.setDelimiterParsingDisabled(true);
				
				//read the template property
				gmlTemplatePath = Paths.get(configuration.getString("template"));
				
				//read the instance property
				for(Object instance : configuration.getList("instances.instance")){
					gmlInstancePaths.add(Paths.get(instance.toString()));
				}
			} catch (ConfigurationException | InvalidPathException e) {
				PrintUsageAndQuit("gml input file - " + e.getMessage());
			}

			// create loader
			loader = new GMLGraphLoader(gmlTemplatePath);

			// create partitioner
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
		}

		// create componentizer
		IGraphComponentizer graphComponentizer = null;
		switch (componentizerMode) {
		case SINGLE:
			graphComponentizer = new SingleComponentizer();
			break;
		case WCC:
			graphComponentizer = new WCCComponentizer();
			break;
		default:
			PrintUsageAndQuit(null);
		}

		// create mapper
		IPartitionMapper partitionMapper = null;
		switch (mapperMode) {
		case ROUNDROBIN:
			partitionMapper = new RoundRobinPartitionMapper(nameNode.getDataNodes());
			break;
		default:
			PrintUsageAndQuit(null);
		}

		// create serializer
		ISliceSerializer serializer = nameNode.getSerializer();
		if (serializer == null) {
			throw new IOException("name node at " + nameNode.getURI() + " returned null serializer");
		}

		// create distributer
		IPartitionDistributer partitionDistributer = null;
		switch (distributerMode) {
		case SCP:
			partitionDistributer = new SCPPartitionDistributer(serializer, instancesGroupingSize, numSubgraphBins);
			break;
		case WRITE:
			partitionDistributer = new DirectWritePartitionDistributer(serializer, instancesGroupingSize, numSubgraphBins);
			break;
		}

		GMLPartitionBuilder partitionBuilder = null;
		try{
			System.out.print("Executing command: DeployGraph");
			for (String arg : args) {
				System.out.print(" " + arg);
			}
			System.out.println();

			// perform deployment
			long time = System.currentTimeMillis();
			switch(partitionedFileMode){
			case DEFAULT:
				partitionBuilder = new GMLPartitionBuilder(graphComponentizer, gmlTemplatePath, gmlInstancePaths);
				deploy(nameNode.getPartitionDirectory(), graphId, numPartitions, loader, partitioner, partitionBuilder, null, partitionMapper, partitionDistributer);
				break;
			case SAVE:
				//save partitioned gml files 
				partitionBuilder = new GMLPartitionBuilder(partitionedGMLFilePath, graphComponentizer, gmlTemplatePath, gmlInstancePaths);
				//partitioned gml input file name format as graphid_numpartitions_paritioningtype_serializer
				String intermediateGMLInputFile = new StringBuffer().append(graphId).append("_").append(numPartitions).append("_")
						.append(partitionerMode.name().toLowerCase()).append("_").append(serializer.getClass().getSimpleName().toLowerCase()).toString();
				deploy(nameNode.getPartitionDirectory(), graphId, numPartitions, loader, partitioner, partitionBuilder, intermediateGMLInputFile, partitionMapper, partitionDistributer);
				break;
			case READ:
				//read partitioned gml files
				partitionBuilder = new GMLPartitionBuilder(graphComponentizer); 
				partitionBuilder.new XMLConfigurationBuilder(gmlInputFile.toFile().getAbsolutePath()).readIntermediateGMLFile();
				deploy(nameNode.getPartitionDirectory(), graphId, numPartitions, partitionBuilder, partitionMapper, partitionDistributer);
				break;
			}

			System.out.println("finished [total " + (System.currentTimeMillis() - time) + "ms]");
		} finally{
			if(partitionBuilder != null)
				partitionBuilder.close();
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
		System.out.println("  GoFSDeployGraph -help");
		System.out.println("      Displays this help message.");
		System.out.println("  GoFSDeployGraph [args] <namenodetype> <namenodeuri> <graphid> <numpartitions> <gmlinputfile>");
		System.out.println("  Args: [-overwriteGraph]");
		System.out.println("        [-partitioner stream|metis[=<pathtometisbinary>[=<metisargs>]]|predefined=<partitioningfile>]");
		System.out.println("        [-intermediategml save[=<pathtopartitionedfiles>]");
		System.out.println("        [-componentizer single|wcc]");
		System.out.println("        [-mapper roundrobin]");
		System.out.println("        [-distributer scp|write]");
		System.out.println("        [-serializer:instancegroupingsize <numinstances>|ALL");
		System.out.println("        [-serializer:numsubgraphbins <numbins>");
		System.out.println();
		System.out.println("      This will partition a graph into sections, and deploy each section to a");
		System.out.println("    data node as specified by the mapper. There are various options for");
		System.out.println("    controlling the method of partitioning, as well as the details of");
		System.out.println("    serialization. The list of data nodes and serialization method will be");
		System.out.println("    picked up from the given name node.");
		System.out.println();
		System.out.println("    Options:");
		System.out.println("      The -overwriteGraph flag indicates that this tool should not warn of a");
		System.out.println("    graph id already in use in the name node and should simply overwrite it.");
		System.out.println("    Note that this does not clean up any physical files associated with the old");
		System.out.println("    graph id, these will simply be lost and will require eventual clean up.");
		System.out.println("      The -partitioner flag selects the partioning to use. Selecting METIS");
		System.out.println("    allows for an optional argument with the path to the metis binary. If not");
		System.out.println("    specified, the binary is assumed to be 'gpmetis' and available on the PATH.");
		System.out.println("    Selecting predefined allows for a required argument, the path to a METIS");
		System.out.println("    style partitioning file. If predefined is selected, the value supplied for");
		System.out.println("    <numpartitions> will be ignored. If the -partitioner flag is not specified,");
		System.out.println("    METIS is used.");
		System.out.println("      The -intermediategml flag indicates that the tool will cache the partitioned gml");
		System.out.println("    files for future use. It will improve the deployment time by using the cached files"); 
		System.out.println("    , instead of partitioning the template and instance gml files again. To read the");
		System.out.println("	partitioned gml files, -intermediategml flag should be specified. If this flag is");
		System.out.println("    not specified, default partitioning mechanism is used.");
		System.out.println("      The -componentizer flag selects the componentizing function to separate");
		System.out.println("    the graph into subgraphs. If the -componentizer flag is not specified, the");
		System.out.println("    wcc function is used, which separates the graph into weakly connected");
		System.out.println("    components.");
		System.out.println("      The -mapper flag selects the mapper to use to map partitions to data nodes");
		System.out.println("    as found in the location input file. If the -mapper flag is not specified,");
		System.out.println("    round robin mapping is used.");
		System.out.println("      The -distributer flag selects the method used to move slices to data nodes.");
		System.out.println("    If SCP is selected, slices will be SCP'ed into position, if WRITE is");
		System.out.println("    selected slices will be written directly into position.");
		System.out.println("      The -serializer:instancegroupingsize flag specifies the number of");
		System.out.println("    instances of group together per slice. If this flag is not specified, a");
		System.out.println("    value of one is used as the default, which will write one instance per");
		System.out.println("    slice. In addition to a number, the special value 'ALL' is accepted to group");
		System.out.println("    all instances into a single slice.");
		System.out.println("      The -serializer:numsubgraphbins flag selects the number of bins to use to");
		System.out.println("    pack subgraphs into slices. If this flag is not specified, a value equal to");
		System.out.println("    the number of subgraphs in the partition is used, which has the effect of");
		System.out.println("    putting each subgraph in its own bin, and thus, writes one subgraph per");
		System.out.println("    slice.");
		System.out.println("      The <namenodetype> is a fully qualified Java class representing the name");
		System.out.println("    node type.");
		System.out.println("      The <namenodeuri> is a uri representing the name node location.");
		System.out.println("      The <graphid> is an user supplied string used to distinguish this graph");
		System.out.println("    from others in the name node.");
		System.out.println("      The <numpartitions> is the number of partitions to create while deploying");
		System.out.println("    the graph.");
		System.out.println("      The <gmlinputfile> is an xml file containing a list of GML files, with root");
		System.out.println("    element as 'gml'. Path to the template GML file can be specified in 'template' element,");
		System.out.println("    and list of instance GML file path can be specified in 'instance' element (with ");
		System.out.println("    'instances' as parent element. For instance packing purposes, instances should be");
		System.out.println("    listed in chronological order within this file for best results. In order to use");
 		System.out.println("    partitioned gml files, path to input file saved in partitioned gml files directory should be");
 		System.out.println("    supplied.");
 		
		System.exit(1);
	}
}
