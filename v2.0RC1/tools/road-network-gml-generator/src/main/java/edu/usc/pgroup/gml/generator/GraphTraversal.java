package edu.usc.pgroup.gml.generator;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

import edu.usc.pgroup.gml.generator.Graph.Edge;
import edu.usc.pgroup.gml.generator.Graph.Node;
import static edu.usc.pgroup.gml.generator.GraphConstants.*;

public class GraphTraversal {
	private static final int MIN_SPEED = 25;
	private static final int MAX_SPEED = 65;
	private static Map<Integer, Node> nodesMap;
	private static Map<ReverseEdge, Property> reverseEdgeMap;
	private static Random random = new Random();

	public static void bfsTraversal(Graph graph, List<Object> speedLimitList, List<Object> distance){
		Property reverseProperty;
		float dist;
		int speedLimit;
		nodesMap = graph.getNodes();
		reverseEdgeMap = new HashMap<GraphTraversal.ReverseEdge, GraphTraversal.Property>();
		//Get the start node
		Iterator<Node> iterator = nodesMap.values().iterator();
		//Traverse the graph
		Node currentNode = null;
		List<Edge> outEdges;
		while(iterator.hasNext()){
			currentNode = iterator.next();
			outEdges = currentNode.getEdges();
			for(Edge edge : outEdges){
				if(edge.getEdgePropertiesMap().size() <= 0){
					reverseProperty = reverseEdgeMap.get(new ReverseEdge(edge.getSink(), edge.getSource()));
					if(reverseProperty != null){
						edge.addEdgeProperty(DISTANCE, reverseProperty.getDist());
						edge.addEdgeProperty(SPEED_LIMIT, reverseProperty.getSpeed());
					}else{
						dist = Float.parseFloat(distance.get(random.nextInt(4)).toString());
						speedLimit = getSpeedLimit(edge, speedLimitList, outEdges);
						edge.addEdgeProperty(DISTANCE, dist);
						edge.addEdgeProperty(SPEED_LIMIT, speedLimit);
						reverseEdgeMap.put(new ReverseEdge(edge.getSource(), edge.getSink()), new Property(speedLimit, dist));
					}
				}	
			}
		}
	}

	private static int getSpeedLimit(Edge currentEdge, List<Object> speedLimit, List<Edge> outEdges){

		int size = outEdges.size();
		Edge inEdge = null;
		if(size > 0){
			boolean firstIteration = false;
			inEdge = outEdges.get(random.nextInt(outEdges.size()));
			//Select a random edge if current and in edge are same
			while(currentEdge.equals(inEdge)){
				if(firstIteration && size <= 1)
					return (int) speedLimit.get(random.nextInt(4));

				inEdge = outEdges.get(random.nextInt(outEdges.size()));
				firstIteration = true;
			}
		}

		Object property = inEdge.getPropertyValue(SPEED_LIMIT);
		Integer value = property == null ? null : (int) property;
		float rFloat;
		int currentIndex;
		int currentSpeed = 0;
		if(value == null){
			currentSpeed = (int) speedLimit.get(random.nextInt(4));
		}else{
			rFloat = random.nextFloat();
			if(rFloat <= 0.6){
				if(value == MIN_SPEED){
					if(random.nextFloat() <= 0.75){
						currentSpeed = value;
					}else{
						currentSpeed = MAX_SPEED;
					}
				}else if(value == MAX_SPEED){
					if(random.nextFloat() <= 0.75){
						currentSpeed = value;
					}else{
						currentSpeed = MIN_SPEED;
					}
				}else{
					currentSpeed = value;
				}
			}else if(rFloat > 0.6 && rFloat <= 0.8 ){
				currentIndex = speedLimit.indexOf(value);
				if(currentIndex < speedLimit.size() - 1)
					currentSpeed = (int) speedLimit.get(currentIndex + 1);
				else
					currentSpeed = value;
			}else{
				currentIndex = speedLimit.indexOf(value);
				if(currentIndex <= 0)
					currentSpeed = value;
				else
					currentSpeed = (int) speedLimit.get(currentIndex - 1);
			}
		}

		return currentSpeed;
	}

	/**
	 * Reverse Edge class to get the same property value on edges
	 */
	static class ReverseEdge {
		private int source;

		private int sink;

		public ReverseEdge(int source, int sink){
			this.source = source;
			this.sink = sink;
		}

		@Override
		public int hashCode() {
			int hash = 7;
			hash = 31 * hash + source + sink;
			hash = 31 * hash + source * sink;
			return hash;
		}

		@Override
		public boolean equals(Object obj) {
			if(obj == null || getClass() != obj.getClass())
				return false;

			ReverseEdge rEdge = (ReverseEdge) obj;
			return (rEdge.sink == this.sink || rEdge.sink == this.source) && (rEdge.source == this.source || rEdge.source == this.sink);
		}
	}

	static class Property{
		private int speed;
		private float dist;

		public Property(int speed, float dist){
			this.speed = speed;
			this.dist = dist;
		}

		public int getSpeed() {
			return speed;
		}

		public float getDist() {
			return dist;
		}
	}
}
