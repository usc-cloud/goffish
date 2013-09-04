package edu.usc.pgroup.goffish.gofs.gml;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;

import edu.usc.pgroup.goffish.gofs.gml.Graph.Edge;
import edu.usc.pgroup.goffish.gofs.gml.Graph.Node;
import static edu.usc.pgroup.goffish.gofs.gml.GraphConstants.*;

public class GMLGenerator {

	private static final String NEW_LINE = "\n";

	private static Path gmlDirPath;

	private static Graph graph;

	private static Map<String, PropertySet> vertexPropertySet;

	private static Map<String, PropertySet> edgePropertySet;

	private static Random random = new Random();
	
	private static void initialize() throws IOException{
		graph = new Graph();
		vertexPropertySet = new  HashMap<>();
		edgePropertySet = new HashMap<>();

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
	
	private static String getPropertyString(String propertyName, int is_static, String propertyType){
		StringBuffer buffer = new StringBuffer();
		buffer.append(propertyName + " [").append(NEW_LINE).append("is_static " + is_static)
			.append(NEW_LINE).append("type " + "\"" + propertyType + "\"").append(NEW_LINE).append("]").append(NEW_LINE);
		
		return buffer.toString();
	}

	private static void writeGraphTemplateFile(String formatType, String fileName, int isDirected) throws IOException{
		Date startTime = new Date();
		System.out.println("Writing the graph template file started.");
		try(PrintWriter writer = new PrintWriter(Files.newOutputStream(gmlDirPath.resolve(fileName)))){
			writer.write("graph [");
			writer.write(NEW_LINE);
			writer.write("directed " + isDirected);
			writer.write(NEW_LINE);
			writer.write("vertex_properties [");
			writer.write(NEW_LINE);

			Set<String> vertexProperties = vertexPropertySet.keySet();
			Set<String> edgeProperties = edgePropertySet.keySet();
			Map<String, Object> vertexPropertiesMap;
			Map<String, Object> edgePropertiesMap;
			Collection<Node> nodes;
			PropertySet propertySet;
			Object value;
			//Write vertex property values
			for(String propertyName : vertexProperties){
				propertySet = vertexPropertySet.get(propertyName);
				writer.write(getPropertyString(propertyName, (propertySet.is_static() ? 1 : 0), propertySet.getType()));
			}

			writer.write("]");
			writer.write(NEW_LINE);

			writer.write("edge_properties [");
			writer.write(NEW_LINE);
			//Write edge property values
			for(String propertyName : edgeProperties){
				propertySet = edgePropertySet.get(propertyName);
				writer.write(getPropertyString(propertyName, (propertySet.is_static() ? 1 : 0), propertySet.getType()));
			}
			
			//Add the dimacs cost property, iff the graph format type is DIMACS
			if(formatType.equals(DIMACS)){
				writer.write(getPropertyString(DIMACS_COST, 1, "integer"));
			}

			writer.write("]");
			writer.write(NEW_LINE);

			nodes = graph.getNodes().values();
			for(Node node : nodes){

				writer.write("node [");
				writer.write(NEW_LINE);
				writer.write("id " + node.getId());
				writer.write(NEW_LINE);

				vertexPropertiesMap = node.getVertexPropertiesMap();
				vertexProperties = vertexPropertiesMap.keySet();
				for(String key : vertexProperties){
					value = vertexPropertiesMap.get(key);
					writer.write(key + " " + value);
					writer.write(NEW_LINE);
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

					edgePropertiesMap = edge.getEdgePropertiesMap();
					edgeProperties = edgePropertiesMap.keySet();
					for(String key : edgeProperties){
						value = edgePropertiesMap.get(key);
						writer.write(key + " " + value);
						writer.write(NEW_LINE);
					}

					writer.write("]");
					writer.write(NEW_LINE);
				}
			}

			writer.write("]");
			writer.write(NEW_LINE);
		}
		System.out.println("Writing graph template file completed. [" + (new Date().getTime() - startTime.getTime()) + " ms]");
	}

	private static void writeGraphInstancesFile(String fileName, Graph instanceGraph) throws IOException{
		try(PrintWriter writer = new PrintWriter(Files.newOutputStream(gmlDirPath.resolve(fileName)))){
			writer.write("graph [");
			writer.write(NEW_LINE);
			writer.write("id " + instanceGraph.getGraphId());
			writer.write(NEW_LINE);
			writer.write("timestamp_start " + instanceGraph.getStartTime());
			writer.write(NEW_LINE);
			writer.write("timestamp_end " + instanceGraph.getEndTime());
			writer.write(NEW_LINE);

			Map<String, Object> vertexPropertiesMap;
			Map<String, Object> edgePropertiesMap;
			Set<String> vertexProperties;
			Set<String> edgeProperties;
			Object value;
			Collection<Node> nodes = instanceGraph.getNodes().values();
			List<Edge> edges;
			for(Node node : nodes){

				writer.write("node [");
				writer.write(NEW_LINE);
				writer.write("id " + node.getId());
				writer.write(NEW_LINE);

				vertexPropertiesMap = node.getVertexPropertiesMap();
				vertexProperties = vertexPropertiesMap.keySet();
				for(String key : vertexProperties){
					value = vertexPropertiesMap.get(key);
					writer.write(key + " " + value);
					writer.write(NEW_LINE);
				}

				writer.write("]");
				writer.write(NEW_LINE);

				edges = node.getEdges();
				for(Edge edge : edges){
					writer.write("edge [");
					writer.write(NEW_LINE);
					writer.write("id " + edge.getEdgeId());
					writer.write(NEW_LINE);
					writer.write("source " + edge.getSource());
					writer.write(NEW_LINE);
					writer.write("target " + edge.getSink());
					writer.write(NEW_LINE);

					edgePropertiesMap = edge.getEdgePropertiesMap();
					edgeProperties = edgePropertiesMap.keySet();
					for(String key : edgeProperties){
						value = edgePropertiesMap.get(key);
						writer.write(key + " " + value);
						writer.write(NEW_LINE);
					}

					writer.write("]");
					writer.write(NEW_LINE);
				}
			}

			writer.write("]");
			writer.write(NEW_LINE);
		}
	}


	private static void loadSnapGraph(Path datasetFileName) throws FileNotFoundException, IOException{
		try(BufferedReader bReader = new BufferedReader(new FileReader(datasetFileName.toFile()))){
			String line;
			int source;
			int sink;
			int edgeId = 1;
			Node sourceNode = null;
			Node sinkNode = null;
			while((line = bReader.readLine()) != null){
				if(!line.startsWith("#")){
					String[] edge_split = line.split("\\s+");
					source = Integer.parseInt(edge_split[0]) + 1;
					sink = Integer.parseInt(edge_split[1]) + 1;

					if(!graph.containsNode(source)){
						sourceNode = new Node(source);
						graph.addNode(source, sourceNode);
					}

					if(!graph.containsNode(sink)){
						sinkNode = new Node(sink);
						graph.addNode(sink, sinkNode);
					}

					//Add the edge in graph object
					Edge edge = new Edge(edgeId, source, sink);
					edgeId++;

					sourceNode.addEdge(edge);
				}
			}
		}
	}

	private static void loadDimacsGraph(Path datasetFileName) throws FileNotFoundException, IOException{
		try(BufferedReader bReader = new BufferedReader(new FileReader(datasetFileName.toFile()))){
			String line;
			int source;
			int sink;
			int edgeId = 1;
			Node sourceNode = null;
			Node sinkNode = null;
			while((line = bReader.readLine()) != null){
				if(line.startsWith(DIMACS_START_LINE)){
					String[] edge_split = line.split("\\s+");
					source = Integer.parseInt(edge_split[1]) + 1;
					sink = Integer.parseInt(edge_split[2]) + 1;

					if(!graph.containsNode(source)){
						sourceNode = new Node(source);
						graph.addNode(source, sourceNode);
					}

					if(!graph.containsNode(sink)){
						sinkNode = new Node(sink);
						graph.addNode(sink, sinkNode);
					}

					//Add the edge in graph object
					Edge edge = new Edge(edgeId, source, sink);
					if(edge_split.length >= 4)
						edge.addEdgeProperty(DIMACS_COST, edge_split[3]);
					edgeId++;

					sourceNode.addEdge(edge);
				}
			}
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

	private static List<Object> randomStringValues(long values){
		List<Object> stringValues = new ArrayList<>();
		for(int index = 0; index < values; index++){
			stringValues.add("\"" + UUID.randomUUID() + "\"");
		}

		return stringValues;
	}

	private static List<Object> randomIntegerValues(long values){
		List<Object> integerValues = new ArrayList<>();
		for(int index = 0; index < values; index++){
			integerValues.add(random.nextInt(5000) + 100);
		}

		return integerValues;
	}

	private static List<Object> randomFloatValues(long values){
		List<Object> floatValues = new ArrayList<>();
		for(int index = 0; index < values; index++){
			floatValues.add((float)(random.nextFloat() * (1000.0 - 10.0)+ 10.0));
		}

		return floatValues;
	}

	private static List<Long> randomTimeValues(int range){
		List<Long> timeValues = new ArrayList<>();
		long inittime = new Date().getTime();
		for(int index = 0; index < range; index++){
			timeValues.add(inittime);
			inittime += 300000;
		}

		return timeValues;
	}

	/**
	 * @param args
	 * @throws IOException 
	 * @throws InterruptedException 
	 * @throws CloneNotSupportedException 
	 */
	public static void main(String[] args) throws IOException, InterruptedException, CloneNotSupportedException {

		if(args.length < 12){
			System.err.println("USAGE: GMLGenerator -type <dataset/type> -input <dataset/file/name> -directed <directed> -properties <config/properties>" +
					" -instances <no/of/instances> -out <output/directory>");
			System.err.println("<dataset/type> - SNAP or DIMACS Graph");
			System.err.println("<dataset/file/name> - Input graph file");
			System.err.println("<directed> - true, If graph is directed");
			System.err.println("<config/properties> - Config file to specify vertex and edge properties");
			System.err.println("<no/of/instances> - Number of graph instances to be generated");
			System.err.println("<output/directory> - Output directory to generate GML files");
			System.exit(0);
		}
		
		Path datasetFilePath = null;
		String formatType = null;
		int isDirected = 0;
		String configProperties = null;
		int numberInstances = 0;
		for(int index = 0; index < args.length; index++){
			switch(args[index]){
			case "-type":
				formatType = args[++index];
				break;
			case "-input":
				datasetFilePath = Paths.get(args[++index]);
				break;
			case "-directed":
				isDirected = Boolean.valueOf(args[++index]) ? 1 : 0;
				break;
			case "-properties":
				configProperties = args[++index];
				break;
			case "-instances":
				numberInstances = Integer.parseInt(args[++index]);
				break;
			case "-out":
				gmlDirPath = Paths.get(args[++index]);
				break;
			}
		}
 
		//initialize the gml properties
		initialize();

		//Read the properties from config file
		try(BufferedReader configReader = new BufferedReader(new FileReader(configProperties))){
			String line;
			while((line = configReader.readLine()) != null){
				String[] propertyValues = line.split(",");
				Boolean is_static = Boolean.valueOf(propertyValues[2]);
				String type = propertyValues[3].toLowerCase();
				String propertyname = propertyValues[1].toLowerCase();
				if(line.startsWith("vertex")){
					vertexPropertySet.put(propertyname, generatePropertySet(propertyname, type, is_static, NODE_VALUES));
				}else if(line.startsWith("edge")){
					edgePropertySet.put(propertyname, generatePropertySet(propertyname, type, is_static,  EDGE_VALUES));
				}
			}
		}

		Date startTime;
		if(formatType.equals(SNAP)){
			startTime = new Date();
			System.out.println("Loading the SNAP graph dataset.");
			loadSnapGraph(datasetFilePath);
			System.out.println("SNAP graph loading completed. [" + (new Date().getTime() - startTime.getTime()) + " ms]");
		}else if(formatType.equals(DIMACS)){
			startTime = new Date();
			System.out.println("Loading the DIMACS graph dataset.");
			loadDimacsGraph(datasetFilePath);
			System.out.println("DIMACS graph loading completed. [" + (new Date().getTime() - startTime.getTime()) + " ms]");
		}

		//Assign values to the graph template
		assignValuesToGraphTemplate(graph);

		//Generating graph template
		writeGraphTemplateFile(formatType, datasetFilePath.getFileName() + "-template.gml", isDirected);

		List<Long> timeValues = randomTimeValues(numberInstances);
		Graph instance;
		long timestampStartTime;
		long timestampEndTime;
		for(int inst_index = 1; inst_index <= numberInstances; inst_index++){
			startTime = new Date();
			System.out.println("Generating the graph instance - " + inst_index + " started.");
			instance = (Graph) graph.clone();

			//Assign values to the graph template
			assignValuesToGraphInstance(instance);
			System.out.println("Generating the graph instance - " + inst_index + " completed. " + (new Date().getTime() - startTime.getTime()) + " ms]");
			instance.setGraphId(inst_index);

			timestampStartTime = timeValues.get(inst_index - 1) + 1;
			timestampEndTime = timestampStartTime + 3000000;
			instance.setStartTime(timestampStartTime);
			instance.setEndTime(timestampEndTime);

			startTime = new Date();
			System.out.println("Writing graph instance - " + inst_index + " file started. ");
			writeGraphInstancesFile(datasetFilePath.getFileName() + "-instance-" + inst_index + ".gml", instance);
			System.out.println("Writing graph instance - " + inst_index + " file completed. " + (new Date().getTime() - startTime.getTime()) + " ms]");
		}
	}

	private static void assignValuesToGraphTemplate(Graph graph) {
		int vIndex = 0;
		int eIndex = 0;
		PropertySet propertySet;

		//Assign values to nodes
		Collection<Node> nodes = graph.getNodes().values();
		Collection<String> vertexPropertiesSet = vertexPropertySet.keySet();
		Collection<String> edgePropertiesSet = edgePropertySet.keySet();
		List<Edge> adjacencyList;
		for(Node node : nodes){
			if(vIndex <= NODE_VALUES - 1)
				vIndex = 0;

			for(String vertexProperty : vertexPropertiesSet){
				propertySet = vertexPropertySet.get(vertexProperty);
				if(random.nextFloat() > 0.3 || propertySet.is_static()){
					node.addVertexProperty(vertexProperty, propertySet.getPropertyValues().get(++vIndex));
				}
			}

			//Assign values to edges
			adjacencyList = node.getEdges();
			for(Edge edge : adjacencyList){
				if(eIndex <= EDGE_VALUES - 1)
					eIndex = 0;

				for(String edgeProperty : edgePropertiesSet){
					propertySet = edgePropertySet.get(edgeProperty);
					if(random.nextFloat() > 0.3 || propertySet.is_static()){
						edge.addEdgeProperty(edgeProperty, propertySet.getPropertyValues().get(++eIndex));
					}
				}
			}
		}
	}

	private static void assignValuesToGraphInstance(Graph graph) {
		int vIndex = 0;
		int eIndex = 0;
		PropertySet propertySet;

		//Assign values to nodes
		Collection<Node> nodes = graph.getNodes().values();
		Collection<String> vertexPropertiesSet = vertexPropertySet.keySet();
		Collection<String> edgePropertiesSet = edgePropertySet.keySet();
		List<Edge> adjacencyList;
		for(Node node : nodes){
			if(vIndex <= NODE_VALUES - 1)
				vIndex = 0;

			for(String vertexProperty : vertexPropertiesSet){
				propertySet = vertexPropertySet.get(vertexProperty);
				if(!propertySet.is_static() && random.nextFloat() > 0.3){
					node.addVertexProperty(vertexProperty, propertySet.getPropertyValues().get(++vIndex));
				}
			}

			//Assign values to edges
			adjacencyList = node.getEdges();
			for(Edge edge : adjacencyList){
				if(eIndex <= EDGE_VALUES - 1)
					eIndex = 0;

				for(String edgeProperty : edgePropertiesSet){
					propertySet = edgePropertySet.get(edgeProperty);
					if(!propertySet.is_static() && random.nextFloat() > 0.3){
						edge.addEdgeProperty(edgeProperty, propertySet.getPropertyValues().get(++eIndex));
					}
				}
			}
		}
	}

	private static PropertySet generatePropertySet(String propertyname, String type,
			boolean is_static, int values) {
		switch(type){
		case "integer":
			return new PropertySet(is_static, type, randomIntegerValues(values));
		case "float":
			return new PropertySet(is_static, type, randomFloatValues(values));
		case "string":
			return new PropertySet(is_static, type, randomStringValues(values));
		default:
			return null;
		}
	}

}
