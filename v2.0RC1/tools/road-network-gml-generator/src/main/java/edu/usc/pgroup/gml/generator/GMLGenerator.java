package edu.usc.pgroup.gml.generator;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import edu.usc.pgroup.gml.generator.Graph.Edge;
import edu.usc.pgroup.gml.generator.Graph.Node;
import static edu.usc.pgroup.gml.generator.GraphConstants.*;

public class GMLGenerator {

	private static final String NEW_LINE = "\n";
	private static final String SPACE = " ";
	private static final DecimalFormat decimalFormat = new DecimalFormat("#.##");
	private static Path gmlDirPath;
	private static Graph graph;
	private static Map<String, PropertySet> vertexPropertySet;
	private static Map<String, PropertySet> edgePropertySet;
	private static Map<Integer, VehicleInfo> vehicleInfoSet;
	private static Map<Integer, VehicleMovement> vehicleMovementSet; 
	private static Map<Integer, List<Integer>> vertexVehicleMapping;
	private static final Random random = new Random();

	public static void initialize() throws IOException{
		gmlDirPath = Paths.get(".").resolve("gml_files");
		graph = new Graph();
		vertexPropertySet = new  HashMap<>();
		edgePropertySet = new HashMap<>();
		vehicleInfoSet = new HashMap<>();
		vehicleMovementSet = new HashMap<>();
		vertexVehicleMapping = new HashMap<>();

		if(!Files.exists(gmlDirPath)){
			Files.createDirectories(gmlDirPath);
		}else{
			recursiveDeleteFiles(gmlDirPath);
		}
	}

	private static void recursiveDeleteFiles(Path dirPath) throws IOException{
		try(DirectoryStream<Path> dirStream = Files.newDirectoryStream(dirPath)){
			for(Path path : dirStream){
				Files.deleteIfExists(path);
			}
		}

	}

	private static void writeGraphTemplateFile(String fileName, int isDirected) throws IOException{
		try(PrintWriter writer = new PrintWriter(Files.newOutputStream(gmlDirPath.resolve(fileName)))){
			writer.write("graph [");
			writer.write(NEW_LINE);
			writer.write("directed " + isDirected);
			writer.write(NEW_LINE);
			writer.write("vertex_properties [");
			writer.write(NEW_LINE);

			//Write vertex property values
			for(String propertyName : vertexPropertySet.keySet()){
				PropertySet valueSet = vertexPropertySet.get(propertyName);
				writer.write(propertyName.toLowerCase() + " [");
				writer.write(NEW_LINE);
				writer.write("is_static " + String.valueOf(valueSet.is_static() ? 1 : 0));
				writer.write(NEW_LINE);
				writer.write("type " + "\"" + valueSet.getType().toLowerCase() + "\"");
				writer.write(NEW_LINE);
				writer.write("]");
				writer.write(NEW_LINE);
			}

			writer.write("]");
			writer.write(NEW_LINE);

			writer.write("edge_properties [");
			writer.write(NEW_LINE);
			//Write edge property values
			for(String propertyName : edgePropertySet.keySet()){
				PropertySet valueSet = edgePropertySet.get(propertyName);
				writer.write(propertyName.toLowerCase() + " [");
				writer.write(NEW_LINE);
				writer.write("is_static " + String.valueOf(valueSet.is_static() ? 1 : 0));
				writer.write(NEW_LINE);
				writer.write("type " + "\"" + valueSet.getType().toLowerCase() + "\"");
				writer.write(NEW_LINE);
				writer.write("]");
				writer.write(NEW_LINE);
			}

			writer.write("]");
			writer.write(NEW_LINE);

			Map<String, Object> propertiesMap;
			Set<String> propertyKeySet;
			String property;
			StringBuffer buffer = new StringBuffer();
			Collection<Node> nodeValues = graph.getNodes().values();
			for(Node node : nodeValues){

				writer.write("node [");
				writer.write(NEW_LINE);
				writer.write("id " + node.getId());
				writer.write(NEW_LINE);

				propertiesMap = node.getMap();
				propertyKeySet = propertiesMap.keySet();

				for(String key : propertyKeySet){
					property = propertiesMap.get(key).toString();
					writer.write(buffer.append(key).append(SPACE).append(property.isEmpty() ? "EMPTY" : property).append(NEW_LINE).toString());
					buffer.setLength(0);
				}

				writer.write("]");
				writer.write(NEW_LINE);

				List<Edge> edges = node.getEdges();
				for(Edge edge : edges){

					writer.write("edge [");
					writer.write(NEW_LINE);
					writer.write("id " + edge.getEdgeId());
					writer.write(NEW_LINE);
					writer.write("source " + edge.getSource());
					writer.write(NEW_LINE);
					writer.write("target " + edge.getSink());
					writer.write(NEW_LINE);

					propertiesMap = edge.getEdgePropertiesMap();
					propertyKeySet = propertiesMap.keySet();

					for(String key : propertyKeySet){
						property = propertiesMap.get(key).toString();
						writer.write(buffer.append(key).append(SPACE).append(property.isEmpty() ? "EMPTY" : property).append(NEW_LINE).toString());
						buffer.setLength(0);
					}


					writer.write("]");
					writer.write(NEW_LINE);
				}
			}

			writer.write("]");
			writer.write(NEW_LINE);
		}
	}

	@SuppressWarnings("unchecked")
	private static void writeGraphInstancesFile(Graph instanceGraph, String fileName) throws IOException{
		try(PrintWriter writer = new PrintWriter(Files.newOutputStream(gmlDirPath.resolve(fileName)))){
			writer.write("graph [");
			writer.write(NEW_LINE);
			writer.write("id " + instanceGraph.getGraphId());
			writer.write(NEW_LINE);
			writer.write("timestamp_start " + instanceGraph.getStartTime());
			writer.write(NEW_LINE);
			writer.write("timestamp_end " + instanceGraph.getEndTime());
			writer.write(NEW_LINE);

			Map<String, Object> propertiesMap;
			Set<String> propertyKeySet;
			List<String> propertyValueList; 
			Object property;
			String propertyStringValue = "";
			StringBuffer buffer = new StringBuffer();
			for(Node node : instanceGraph.getNodes().values()){

				writer.write("node [");
				writer.write(NEW_LINE);
				writer.write("id " + node.getId());
				writer.write(NEW_LINE);

				propertiesMap = node.getMap();
				propertyKeySet = propertiesMap.keySet();

				for(String key : propertyKeySet){
					property = propertiesMap.get(key);
					if(property instanceof String){
						propertyStringValue = property == null || property.toString().isEmpty() ? "EMPTY" : property.toString();
					}else if(property instanceof List<?>){
						propertyValueList = (List<String>) propertiesMap.get(key);
						propertyStringValue = propertyValueList.isEmpty() ? "EMPTY" : getListAsString(propertyValueList);
					}

					writer.write(buffer.append(key).append(SPACE).append("\"").append
							(propertyStringValue).append("\"").append(NEW_LINE).toString());
					buffer.setLength(0);
				}

				writer.write("]");
				writer.write(NEW_LINE);

				for(Edge edge : node.getEdges()){

					writer.write("edge [");
					writer.write(NEW_LINE);
					writer.write("id " + edge.getEdgeId());
					writer.write(NEW_LINE);
					writer.write("source " + edge.getSource());
					writer.write(NEW_LINE);
					writer.write("target " + edge.getSink());
					writer.write(NEW_LINE);

					writer.write("]");
					writer.write(NEW_LINE);
				}
			}

			writer.write("]");
			writer.write(NEW_LINE);
		}
	}


	private static void generateGraphTemplate(Path datasetFileName) throws FileNotFoundException, IOException{
		Date startTime = new Date();
		System.out.println("Generate graph by reading the dataset..");
		try(BufferedReader bReader = new BufferedReader(new FileReader(datasetFileName.toFile()))){
			String line;
			int source;
			int sink;
			int edgeId = 1;
			Node node;
			while((line = bReader.readLine()) != null){
				if(!line.startsWith("#")){
					String[] edge_split = line.split("\\s+");
					source = Integer.parseInt(edge_split[0]) + 1;
					sink = Integer.parseInt(edge_split[1]) + 1;

					//Add the edge in graph object
					if(!graph.containsNode(source)){
						node = new Node(source);
						graph.addNode(node.getId(), node);
						node.addVertexProperty(INTERSECTION_ID, source);
					}

					if(!graph.containsNode(sink)){
						node = new Node(sink);
						graph.addNode(node.getId(), node);
						node.addVertexProperty(INTERSECTION_ID, sink);
					}

					Edge edge = new Edge(edgeId, source, sink);
					edgeId++;

					graph.getNode(source).addEdge(edge);
				}
			}

			System.out.println("Graph generation finished. [" + (new Date().getTime() - startTime.getTime()) + " ms]");
		}
	}

	static class PropertySet {
		private boolean is_static;

		private String type;

		private List<Object> randomPropertyValues;

		public PropertySet(boolean is_static, String type, List<Object> randomPropertyValues){
			this.is_static = is_static;
			this.type = type;
			this.randomPropertyValues = randomPropertyValues;
		}

		public String getType() {
			return type;
		}

		public boolean is_static() {
			return is_static;
		}

		public List<Object> getPropertyValues(){
			return randomPropertyValues;
		}
	}

	private static List<Long> randomTimeValues(int range){
		List<Long> timeValues = new ArrayList<>();
		long inittime = new Date().getTime();
		for(int index = 0; index < range; index++){
			timeValues.add(inittime);
			inittime += 60;
		}

		return timeValues;
	}

	public Graph getGraph() {
		return graph;
	}

	private static void updateVehicleMovement(Graph graph){
		Map<Integer, Node> nodesMap = graph.getNodes();
		//Get the start node
		Collection<Node> nodes = nodesMap.values();

		//Traverse the graph
		List<Edge> outEdges;
		List<Integer> vehicleIndexList;
		float dist;
		int movIndex;
		Set<Integer> keyIndexSet;
		Map<Integer, Float> vehicleTravelledMap;
		for(Node currentNode : nodes){
			outEdges = currentNode.getEdges();
			for(Edge edge : outEdges){
				vehicleIndexList = vertexVehicleMapping.get(currentNode.getId());

				dist = (float) edge.getPropertyValue(DISTANCE);

				vehicleTravelledMap = new HashMap<Integer, Float>(edge.getVehicleTravelledMap());
				edge.getVehicleTravelledMap().clear();
				if(vehicleIndexList != null){
					movIndex = getVehicleMovementIndex(vehicleIndexList, edge);
					if(movIndex >= 0){
						//Time in minute
						moveVehicles(nodesMap, dist, 
								movIndex, edge, 1);
					}
				}

				keyIndexSet = vehicleTravelledMap.keySet();
				for(Integer keyIndex : keyIndexSet){
					moveVehicles(nodesMap, (dist - vehicleTravelledMap.get(keyIndex)), 
							keyIndex, edge, 1);
				}
			}
		}
	}

	private static void moveVehicles(Map<Integer, Node> nodesMap, float distanceToTravel,
			int movIndex, Edge edge, float actualTime) {
		VehicleMovement vehicleMovement;
		int speedLimit, carSpeed;
		float totaldist, distRemaining, timeRemaining
		,timeTaken, distTravelled;
		Edge vehicleOutEdge;

		VehicleInfo vehicleInfo = vehicleInfoSet.get(movIndex);
		Node sinkNode = nodesMap.get(edge.getSink());
		List<Edge> sinkEdges = sinkNode.getEdges();
		int size = sinkEdges.size();
		totaldist = (float) edge.getPropertyValue(DISTANCE);
		speedLimit = (int) edge.getPropertyValue(SPEED_LIMIT);

		carSpeed = getCarSpeed(speedLimit);
		timeTaken = Float.parseFloat(decimalFormat.format((distanceToTravel/ carSpeed) * 60));
		vehicleMovement = vehicleMovementSet.get(movIndex);
		if(timeTaken >= 1){
			distTravelled = Float.parseFloat(decimalFormat.format(carSpeed * actualTime/60));
			distRemaining = totaldist - distTravelled;
			if(distRemaining <= 0.25){
				vehicleOutEdge = getVehicleOutEdge(edge, sinkEdges);

				vehicleOutEdge = (vehicleOutEdge == null) ? ((size > 0) ? sinkEdges.get(0) : null) : vehicleOutEdge;
				if(vehicleOutEdge != null){
					addVehicleMovementAsProperties(nodesMap, vehicleInfo, vehicleMovement,
							vehicleOutEdge.getSource(), vehicleOutEdge.getSink(), vehicleOutEdge.getEdgeId());
				}
			}else{
				edge.addVehicleTravelled(movIndex, distTravelled);
			}
		}else{
			vehicleOutEdge = getVehicleOutEdge(edge, sinkEdges);
			timeRemaining = Float.parseFloat(decimalFormat.format(1.0f - timeTaken));

			if(vehicleOutEdge != null){
				moveVehicles(nodesMap, (float)vehicleOutEdge.getPropertyValue(DISTANCE), movIndex, vehicleOutEdge, timeRemaining);
			}else{
				if(size > 0){
					vehicleOutEdge = sinkEdges.get(0);
					addVehicleMovementAsProperties(nodesMap, vehicleInfo, vehicleMovement,
							vehicleOutEdge.getSource(), vehicleOutEdge.getSink(), vehicleOutEdge.getEdgeId());
				}
			}
		}
	}

	private static void addVehicleMovementAsProperties(Map<Integer, Node> nodesMap, VehicleInfo vehicleInfo,
			VehicleMovement vehicleMovement, int source, int sink, int edgeId) {
		Node vehicleOutNode;
		vehicleMovement.setEdgeId(edgeId);
		vehicleMovement.setSink(sink);
		vehicleMovement.setSource(source);

		vehicleOutNode = nodesMap.get(source);
		addProperty(vehicleOutNode, LICENSE_PLATE, vehicleInfo.getLicensePlate());
		addProperty(vehicleOutNode, LICENSE_STATE, vehicleInfo.getLicenseState());
		addProperty(vehicleOutNode, VEHICLE_COLOR, vehicleInfo.getVehicleColor());
		addProperty(vehicleOutNode, VEHICLE_TYPE, vehicleInfo.getVehicleType());
	}

	private static Edge getVehicleOutEdge(Edge edge, List<Edge> sinkEdges) {
		Edge vehicleOutEdge;
		int size = sinkEdges.size();
		if(size > 0){
			vehicleOutEdge = sinkEdges.get(random.nextInt(size));
			if(size == 1){
				if(edge.getSource() == vehicleOutEdge.getSink() && edge.getSink() == vehicleOutEdge.getSource())
					return null;
			}else{
				while(edge.getSource() == vehicleOutEdge.getSink() && edge.getSink() == vehicleOutEdge.getSource()){
					vehicleOutEdge = sinkEdges.get(random.nextInt(size));
				}
			}

			return vehicleOutEdge;
		}

		return null;
	}

	@SuppressWarnings("unchecked")
	private static void addProperty(Node node, String propertyType, String value){
		Object propertyValue = node.getPropertyValue(propertyType);
		List<String> valueList;
		if(propertyValue == null){
			valueList = new ArrayList<String>();
			node.addVertexProperty(propertyType, valueList);
		}else{
			valueList = (List<String>) propertyValue;
		}

		valueList.add(value);
	}

	private static int getCarSpeed(int speedlimit){
		float rFloat = random.nextFloat();
		if(rFloat < 0.3)
			return (int) (speedlimit - speedlimit * 0.1);
		else if(rFloat >= 0.3 && rFloat < 0.6)
			return speedlimit;
		else
			return (int) (speedlimit + speedlimit * 0.1);
	}

	private static int getVehicleMovementIndex(List<Integer> vehicleIndexList, Edge edge){
		VehicleMovement vehicleMovement;
		for(Integer vehicleInfoIndex : vehicleIndexList){
			vehicleMovement = vehicleMovementSet.get(vehicleInfoIndex);
			if(vehicleMovement.getEdgeId() == edge.getEdgeId()){
				return vehicleInfoIndex;
			}
		}

		return -1;
	}

	/**
	 * @param args
	 * @throws CloneNotSupportedException 
	 * @throws IOException 
	 */
	public static void main(String[] args) throws CloneNotSupportedException, IOException {

		if(args.length < 3){
			System.err.println("USAGE: GMLGenerator -input <dataset/file/name> -directed <directed> -instances <no/of/instances> -out <output/directory>");
			System.err.println("<dataset/file/name> - Input graph file");
			System.err.println("<directed> - true, If graph is directed");
			System.err.println("<no/of/instances> - Number of graph instances to be generated");
			System.err.println("<output/directory> - Output directory to generate GML files");
			System.exit(0);
		}
		Date startTime;
		Path datasetFilePath = null;
		int isDirected = 0;
		int numberInstances = 0;
		for(int index = 0; index < args.length; index++){
			switch(args[index]){
			case "-input":
				datasetFilePath = Paths.get(args[++index]);
				break;
			case "-directed":
				isDirected = Boolean.valueOf(args[++index]) ? 1 : 0;
				break;
			case "-instances":
				numberInstances = Integer.parseInt(args[++index]);
				break;
			case "-out":
				gmlDirPath = Paths.get(args[++index]);
				break;
			}
		}

		//Initialize the road network dataset
		initialize();

		//Generate random vertex and edge property values
		startTime = new Date();
		System.out.println("Generate the vertex and edge properties");
		generateVertexPropertySet();
		generateEdgePropertySet();
		System.out.println("Property creation ends. [" + (new Date().getTime() - startTime.getTime()) + " ms]");

		//Load the graph template
		generateGraphTemplate(datasetFilePath);

		//Generating graph template properties
		startTime = new Date();
		System.out.println("Assign property values to vertex and edges in graph template");
		GraphTraversal.bfsTraversal(graph, edgePropertySet.get(SPEED_LIMIT).getPropertyValues(), edgePropertySet.get(DISTANCE).getPropertyValues());
		System.out.println("Completed assigning vertex and edge properties in graph template. [" + (new Date().getTime() - startTime.getTime()) + " ms]");

		//Write template
		System.out.println("Writing graph template started.");
		writeGraphTemplateFile(datasetFilePath.getFileName() + "-template.gml", isDirected);
		System.out.println("Writing graph template ends.. [" + (new Date().getTime() - startTime.getTime()) + " ms]");

		//Clone the graph
		Graph instanceGraph = (Graph) graph.clone();

		//Generate vehicle info set
		startTime = new Date();
		System.out.println("Generate the vehicle info set.");
		generateVehicleInfoSet();
		System.out.println("Generating vehicle info ends. [" + (new Date().getTime() - startTime.getTime()) + " ms]");

		//Assign random vehicles to the vertices
		System.out.println("Assign the vechicles to the vertices.");
		assignVehiclesToVertices(instanceGraph);
		System.out.println("Assigning the vechicles to the vertices completed. [" + (new Date().getTime() - startTime.getTime()) + " ms]");

		List<Long> timeValues = randomTimeValues(numberInstances);

		long startInstTime;
		long endInstTime;
		//Write the graph for first instance
		startInstTime = timeValues.get(0) + 1;
		endInstTime = startInstTime + 60;
		instanceGraph.setStartTime(startInstTime);
		instanceGraph.setEndTime(endInstTime);

		System.out.println("Writing the graph instance - " + 1 + " starts.");
		writeGraphInstancesFile(instanceGraph, datasetFilePath.getFileName() + "-instance-" + 1 + ".gml");
		System.out.println("Writing the graph instance - " + 1 + " ends.  [" + (new Date().getTime() - startTime.getTime()) + " ms]");

		//Write the rest of the instances
		for(int inst_index = 2; inst_index <= numberInstances; inst_index++){
			startTime = new Date();
			System.out.println("Cloning the graph to generate graph instance - " + inst_index + ".");
			instanceGraph = (Graph) instanceGraph.clone();
			System.out.println("Cloning the graph to generate graph instance -" + inst_index + " completed.  [" + (new Date().getTime() - startTime.getTime()) + " ms]");

			startTime = new Date();
			System.out.println("Move the vehicles to generate the graph instance - " + inst_index );
			updateVehicleMovement(instanceGraph);
			System.out.println("Moving the vehicles in graph instance - " + inst_index + " completed.  [" + (new Date().getTime() - startTime.getTime()) + " ms]");

			instanceGraph.setGraphId(inst_index);

			startInstTime = timeValues.get(inst_index - 1) + 1;
			endInstTime = startInstTime + 60;
			instanceGraph.setStartTime(startInstTime);
			instanceGraph.setEndTime(endInstTime);

			startTime = new Date();
			System.out.println("Writing the graph instance - " + inst_index + " started.");
			writeGraphInstancesFile(instanceGraph, datasetFilePath.getFileName() + "-instance-" + inst_index + ".gml");
			System.out.println("Writing the graph instance - " + inst_index + " ends.  [" + (new Date().getTime() - startTime.getTime()) + " ms]");
		}
	}

	private static void generateVehicleInfoSet(){
		Object[] platePropertyValues = vertexPropertySet.get(LICENSE_PLATE).getPropertyValues().toArray();
		Object[] statePropertyValues = vertexPropertySet.get(LICENSE_STATE).getPropertyValues().toArray();
		Object[] typePropertyValues = vertexPropertySet.get(VEHICLE_TYPE).getPropertyValues().toArray();
		Object[] colorPropertyValues = vertexPropertySet.get(VEHICLE_COLOR).getPropertyValues().toArray();
		for(int index = 0; index < 10000000; index++){
			vehicleInfoSet.put(index, new VehicleInfo(statePropertyValues[random.nextInt(5)].toString(), platePropertyValues[index].toString(),
					typePropertyValues[random.nextInt(5)].toString(), colorPropertyValues[random.nextInt(8)].toString()));
		}
	}

	private static void assignVehiclesToVertices(Graph graph){
		Map<Integer, Node> nodesMap = graph.getNodes();
		Iterator<Node> iterator = nodesMap.values().iterator();
		int index = 0;
		Node currentNode;
		int sink, edgeId, rInt;
		List<Edge> outEdges;
		VehicleInfo vehicleInfo;
		List<String> vehicleColor = new ArrayList<>();
		List<String> vehicleType = new ArrayList<>();
		List<String> licensePlate = new ArrayList<>();
		List<String> licenseState = new ArrayList<>();
		List<Integer> vehicleIndexList;
		while(iterator.hasNext()){
			currentNode = iterator.next();
			outEdges = currentNode.getEdges();
			vehicleIndexList = new ArrayList<>();
			for(Edge edge : outEdges){
				sink = edge.getSink();
				edgeId = edge.getEdgeId();
				rInt = random.nextInt(2) + 1;
				for(int rIndex = 0; rIndex < rInt && index < 10000000; rIndex++){
					vehicleMovementSet.put(index,  new VehicleMovement(currentNode.getId(), sink, edgeId));
					vehicleIndexList.add(index);
					vehicleInfo = vehicleInfoSet.get(index);

					vehicleColor.add(vehicleInfo.getVehicleColor());
					vehicleType.add(vehicleInfo.getVehicleType());
					licensePlate.add(vehicleInfo.getLicensePlate());
					licenseState.add(vehicleInfo.getLicenseState());
					index++;
				}
				vertexVehicleMapping.put(currentNode.getId(), vehicleIndexList);

			}

			currentNode.addVertexProperty(LICENSE_PLATE, getListAsString(licensePlate));
			currentNode.addVertexProperty(LICENSE_STATE, getListAsString(licenseState));
			currentNode.addVertexProperty(VEHICLE_COLOR, getListAsString(vehicleColor));
			currentNode.addVertexProperty(VEHICLE_TYPE, getListAsString(vehicleType));

			//Clear the list
			licensePlate.clear();
			licenseState.clear();
			vehicleColor.clear();
			vehicleType.clear();
		}

	}

	private static String getListAsString(List<String> valueList){
		Iterator<String> iterator = valueList.iterator();
		StringBuffer buffer = new StringBuffer();
		if(iterator.hasNext()){
			buffer.append(iterator.next());
		}

		while(iterator.hasNext()){
			buffer.append(",").append(iterator.next());
		}

		return buffer.toString();
	}

	private static void generateVertexPropertySet(){
		{
			List<Object> statePropertyValues = new LinkedList<>();
			statePropertyValues.add("CA");
			statePropertyValues.add("NV");
			statePropertyValues.add("OR");
			statePropertyValues.add("AZ");
			statePropertyValues.add("WA");
			vertexPropertySet.put(LICENSE_STATE, new PropertySet(false, "string", statePropertyValues));
		}
		{
			List<Object> platePropertyValues = new LinkedList<>();
			char[] letters = {'A','B','C','D','E','F','G','H','I','J','K','L','M','N','O','P','Q','R','S','T','U','V','W','X','Y','Z'};
			StringBuffer buffer = new StringBuffer();
			List<Character> plateChars = new ArrayList<>();
			Iterator<Character> plateCharsIterator;
			int rInt;
			int rIndex;
			for(int index=0; index < 10000000; index++){
				for(rIndex = 0; rIndex < 3; rIndex++){
					rInt = random.nextInt(26);
					plateChars.add(letters[rInt]);
				}
				for(rIndex = 0; rIndex < 4; rIndex++){
					plateChars.add(Character.forDigit(random.nextInt(10), 10));
				}

				Collections.shuffle(plateChars);
				plateCharsIterator = plateChars.iterator();
				while(plateCharsIterator.hasNext()){
					buffer.append(plateCharsIterator.next());
				}
				platePropertyValues.add(buffer.toString());
				buffer.setLength(0);
				plateChars.clear();
			}
			vertexPropertySet.put(LICENSE_PLATE, new PropertySet(false, "string", platePropertyValues));
		}
		{
			List<Object> typePropertyValues = new LinkedList<>();
			typePropertyValues.add("sedan");
			typePropertyValues.add("pickup");
			typePropertyValues.add("SUV");
			typePropertyValues.add("bus");
			typePropertyValues.add("truck");
			typePropertyValues.add("motorcycle");
			vertexPropertySet.put(VEHICLE_TYPE, new PropertySet(false, "string", typePropertyValues));
		}
		{
			List<Object> colorPropertyValues = new LinkedList<>();
			colorPropertyValues.add("white");
			colorPropertyValues.add("black");
			colorPropertyValues.add("silver");
			colorPropertyValues.add("red");
			colorPropertyValues.add("blue");
			colorPropertyValues.add("brown");
			colorPropertyValues.add("yellow");
			colorPropertyValues.add("green");
			vertexPropertySet.put(VEHICLE_COLOR, new PropertySet(false, "string", colorPropertyValues));
		}
	}

	private static void generateEdgePropertySet(){
		{
			List<Object> limitPropertyValues = new LinkedList<>();
			limitPropertyValues.add(25);
			limitPropertyValues.add(35);
			limitPropertyValues.add(55);
			limitPropertyValues.add(65);
			edgePropertySet.put(SPEED_LIMIT, new PropertySet(true, "integer", limitPropertyValues));
		}
		{
			List<Object> distPropertyValues = new LinkedList<>();
			distPropertyValues.add(0.5);
			distPropertyValues.add(1);
			distPropertyValues.add(2);
			distPropertyValues.add(3);
			edgePropertySet.put(DISTANCE, new PropertySet(true, "float", distPropertyValues));
		}
	}
}
