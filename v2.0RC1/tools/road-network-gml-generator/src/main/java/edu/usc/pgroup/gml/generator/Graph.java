package edu.usc.pgroup.gml.generator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Graph implements Cloneable{

	private Map<Integer, Node> nodes;
	
	private int graphId;
	
	private long startTime;
	
	private long endTime;

	public Graph(){
		nodes = new HashMap<Integer, Node>();
	}
	
	public Graph(int graphId, long startTime, long endTime){
		this();
		this.graphId = graphId;
		this.startTime = startTime;
		this.endTime = endTime;
	}
	
	public void addNode(int nodeId, Node node){
		nodes.put(nodeId, node);
	}
	
	public Node getNode(int nodeId){
		return nodes.get(nodeId);
	}

	public boolean containsNode(int nodeId){
		return nodes.containsKey(nodeId);
	}

	public Map<Integer, Node> getNodes(){
		return nodes;
	}
	
	public void setGraphId(int graphId){
		this.graphId = graphId;
	}
	
	public int getGraphId(){
		return graphId;
	}
	
	public void setStartTime(long startTime){
		this.startTime = startTime;
	}
	
	public long getStartTime(){
		return startTime;
	}
	
	public void setEndTime(long endTime){
		this.endTime = endTime;
	}
	
	public long getEndTime(){
		return endTime;
	}

	public Object clone() throws CloneNotSupportedException {
		Graph graph = new Graph(graphId, startTime, endTime);

		graph.nodes = new HashMap<>();
		Collection<Node> nodes = this.nodes.values();
		List<Edge> edges;
		for(Node node : nodes){
			Node copyNode = (Node)node.clone();
			
			edges = node.getEdges();
			for(Edge edge : edges){
				Edge copyEdge = (Edge)edge.clone();
				copyNode.addEdge(copyEdge);
			}

			graph.addNode(copyNode.getId(), copyNode);
		}

		return graph;
	}

	static class Node implements Cloneable{

		private int vertexId;
		
		private List<Edge> adjacencyList;

		private Map<String, Object> vertexPropertiesMap;

		public Node(int vertexId){
			this.vertexId = vertexId;
			this.vertexPropertiesMap = new HashMap<String, Object>();
			this.adjacencyList = new ArrayList<Edge>();
		}

		public int getId(){
			return vertexId;
		}

		public void addVertexProperty(String key, Object value){
			vertexPropertiesMap.put(key, value);
		}

		public Map<String, Object> getMap(){
			return vertexPropertiesMap;
		}
		
		public Object getPropertyValue(String key){
			return vertexPropertiesMap.get(key);
		}
		
		public void addEdge(Edge e){
			adjacencyList.add(e);
		}
		
		public List<Edge> getEdges(){
			return adjacencyList;
		}

		@Override
		public boolean equals(Object obj) {
			if(obj == null)
				return false;
			if(this == obj)
				return true;
			if(this.getClass() != obj.getClass())
				return false;

			Node other = (Node) obj;

			return this.vertexId == other.getId();
		}

		@Override
		protected Object clone() throws CloneNotSupportedException {
			return new Node(this.vertexId);
		}
	}

	static class Edge implements Cloneable{
		private int source;

		private int edgeId;

		private int sink;

		private Map<String, Object> edgePropertiesMap;
		
		private Map<Integer, Float> vehicleTravelledMap;

		public Edge(int edgeId, int source, int sink){
			this.edgeId = edgeId;
			this.source = source;
			this.sink = sink;
			this.edgePropertiesMap = new HashMap<>();
			this.vehicleTravelledMap = new HashMap<>();
		}

		public int getSource() {
			return source;
		}

		public void setSource(int source) {
			this.source = source;
		}

		public int getEdgeId() {
			return edgeId;
		}

		public void setEdgeId(int edgeId) {
			this.edgeId = edgeId;
		}

		public int getSink() {
			return sink;
		}

		public void setSink(int sink) {
			this.sink = sink;
		}

		public void addEdgeProperty(String key, Object value){
			edgePropertiesMap.put(key, value);
		}
		
		public Object getPropertyValue(String key){
			return edgePropertiesMap.get(key);
		}

		public Map<String, Object> getEdgePropertiesMap(){
			return edgePropertiesMap;
		}
		
		public void addVehicleTravelled(Integer key, Float value){
			vehicleTravelledMap.put(key, value);
		}
		
		public Float getVehicleTravelledDist(Integer key){
			return vehicleTravelledMap.get(key);
		}
		
		public void removeVehicle(Integer key){
			vehicleTravelledMap.remove(key);
		}
		
		public Map<Integer, Float> getVehicleTravelledMap(){
			return vehicleTravelledMap;
		}

		@Override
		protected Object clone() throws CloneNotSupportedException {
			Edge edge = new Edge(this.edgeId, this.source, this.sink);
			
			edge.edgePropertiesMap.putAll(this.edgePropertiesMap);
			edge.vehicleTravelledMap.putAll(this.vehicleTravelledMap);
			return edge;
		}

		@Override
		public boolean equals(Object obj) {
			if (obj == null)
				return false;
			if(this == obj)
				return true;
			if(this.getClass() != obj.getClass())
				return false;

			Edge other = (Edge) obj;

			return this.edgeId == other.getEdgeId() && this.source == other.getSource() && this.sink == other.getSink();
		}

		@Override
		public String toString() {
			return "Edge [" + source + "->" + sink + "]";
		}
	}
}
