/*
 *  Copyright 2013 University of Southern California
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.package edu.usc.goffish.gopher.sample;
 */
package edu.usc.goffish.gopher.sample;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;

import edu.usc.goffish.gofs.ITemplateEdge;
import edu.usc.goffish.gofs.ITemplateVertex;
import edu.usc.goffish.gopher.api.GopherSubGraph;
import edu.usc.goffish.gopher.api.SubGraphMessage;

/***
 * Calculates single source shortest path from a single source to every other vertex 
 * in the graph. Uses just templates (edge weight = 1).
 * Uses Dikstra/A* algorithm for local calculation within subgraph.
 * When local parents of remote vertices are updated by dikstras', we send remote messages 
 * with the updated local parent's distance to remote vertex. 
 * We halt when there are no update messages sent to remote vertices.
 * At the end of all supersteps, every vertex has the shortest distance from the source 
 * vertex and the parent vertex used to arrive on the shortest path.
 * 
 * @author simmhan
 *
 */
public class SingleSourceShortestPathOnTemplate extends GopherSubGraph {

	// Input Variables
    private long sourceVertexID; 

    // Output Variables
    // Output shortest distance map
    private Map<Long, DistanceParentPair> shortestDistanceMap;
    
    // dir location where distance results and parents are saved
    private static Path logRootDir = Paths.get("."); 
    private String logFileName = null;
    //private SimpleDateFormat FORMATTER = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
    
	// Number of remote vertices out of this subgraph. Used for initializing hashmap.
	private int remoteVertexCount;
	private static int verbosity = -1;
	long partitionId, subgraphId;	

    /***
     * Helper class that contains the shortest known distance and 
     * the parent that leads to it
     * 
     * @author simmhan
     *
     */
    private static class DistanceParentPair {
    	public short distance;
    	public long parent;
    	
    	public DistanceParentPair(short distance_, long parent_) {
    		parent = parent_;
    		distance = distance_;
    	}

    	public String toString(){
    		return distance + "," + parent;
    	}    	
    }

    /***
     * Helper class for items in a sorted priority queue of current vertices that 
     * need to be checked for their new distance
     * 
     * @author simmhan
     *
     */
    private static class DistanceVertex implements Comparable<DistanceVertex> {
    	public short distance;
    	public ITemplateVertex vertex;
    	
    	public DistanceVertex(ITemplateVertex vertex_, short distance_) {
    		vertex = vertex_;
    		distance = distance_;
    	}
    	
		@Override
		public int compareTo(DistanceVertex o) {
			return distance - o.distance;
		}
    }
    

    /***
     * MAIN COMPUTE METHOD
     */
    @Override
    public void compute(List<SubGraphMessage> packedSubGraphMessages) {
    	try{
		// init IDs for logging
    	// FIXME: Charith, we need an init() method later on 
    	if(superStep == 0) {
    		partitionId = partition.getId();
        	subgraphId = subgraph.getId();
        	logFileName = "SP_" + partitionId + "_" + subgraphId + ".log";
    	}
    	
		log("START superstep with received input messages count = " + packedSubGraphMessages.size()); 

		Set<ITemplateVertex> rootVertices = null;

		///////////////////////////////////////////////////////////
    	// First superstep. Get source superstep as input.
		// Initialize distances. calculate shortest distances in subgraph.
    	if(superStep == 0) {

    		// get input variables from init message
    		if(packedSubGraphMessages.size() == 0) {
    			throw new RuntimeException("Initial subgraph message was missing! Require sourceVertexID to be passed");
    		}
    		
    	    sourceVertexID = Long.parseLong(new String(packedSubGraphMessages.get(0).getData()));
    	    
    		log("Initializing source vertex = " + sourceVertexID);

    		// Giraph:SimpleShortestPathsComputation.java:64 
    		//   vertex.setValue(new DoubleWritable(Double.MAX_VALUE));
    		
    		// initialize distance map of vertices to infinity
    		// Note that if it is a remote vertex, we only have an estimate of the distance
    		shortestDistanceMap = new HashMap<Long, DistanceParentPair>(subgraph.numVertices());
    		for(ITemplateVertex v : subgraph.vertices()){
    			shortestDistanceMap.put(v.getId(), new DistanceParentPair(Short.MAX_VALUE, -1));
    			if(v.isRemote()) remoteVertexCount++;
    		}
    		
    		// Giraph:SimpleShortestPathsComputation.java:66
    		//    double minDist = isSource(vertex) ? 0d : Double.MAX_VALUE;
    		// Update distance to source as 0
    		boolean subgraphHasSource = false;
    		if(shortestDistanceMap.containsKey(sourceVertexID) && 
    				!subgraph.getVertex(sourceVertexID).isRemote()){
    			shortestDistanceMap.put(sourceVertexID, new DistanceParentPair((short)0, -1));
    			subgraphHasSource = true;
    		}

    		// If we have the source...
    		if(subgraphHasSource){
    			log("We have the source!");
	    		ITemplateVertex sourceVertex = subgraph.getVertex(sourceVertexID);
	    		rootVertices = new HashSet<>(1);
	    		rootVertices.add(sourceVertex);
    		}
    		
        } else {
	    	///////////////////////////////////////////////////////////
	    	// second superstep. 

        	List<String> subGraphMessages = unpackSubgraphMessages(packedSubGraphMessages);
    		log("Unpacked messages count = " + subGraphMessages.size());
    		

        	// We expect no more unique vertices than the number of input messages, 
        	// or the total number of vertices. Note that we are likely over allocating.
        	// For directed graphs, it is not easy to find the number of in-boundary vertices.    		
        	rootVertices = new HashSet<>(Math.min(subGraphMessages.size(),subgraph.numVertices()));
    		
    		// Giraph:SimpleShortestPathsComputation.java:68
    		//     minDist = Math.min(minDist, message.get());

    		// parse messages
            // update distance map using messages if it has improved
    		// add the *unique set* of improved vertices to traversal list
            for (String message : subGraphMessages) {
            	String[] tokens  = message.split(",");    		
        		if(tokens.length != 3) {
        			throw new RuntimeException("Intermediate subgraph message did not contain 3 tokens. Has " + tokens.length + "instead");
        		}
        		long sinkVertex = Long.parseLong(tokens[0]);
        		short sinkDistance = Short.parseShort(tokens[1]);
        		long remoteParent  = Long.parseLong(tokens[2]);
        		DistanceParentPair distanceParent = shortestDistanceMap.get(sinkVertex);
        		if(distanceParent.distance > sinkDistance){ 
        			// path from remote is better than locally known path
        			distanceParent.distance = sinkDistance;
        			distanceParent.parent = remoteParent;
        			rootVertices.add(subgraph.getVertex(sinkVertex));
        		}
            }
        }

    	
		// Giraph:SimpleShortestPathsComputation.java:74
    	//     if (minDist < vertex.getValue().get()) {
    	//       vertex.setValue(new DoubleWritable(minDist));
    	//       for (Edge<LongWritable, FloatWritable> edge : vertex.getEdges()) {
        //         double distance = minDist + edge.getValue().get();
    	//       
    	
    	// if there are changes, then run dikstras
		int changeCount = 0;
		int messageCount = 0;
        if(rootVertices != null && rootVertices.size() > 0) {
    		// List of remote vertices which could be affected by changes to distance
    		// This does local agg that eliminates sending min dist to same vertex from 
			// multiple vertices in this SG
    		Set<Long> remoteUpdateSet = new HashSet<Long>(remoteVertexCount); 

			log("START diskstras. We have source vertex or distances have changed.");

    		// Update distances within local subgraph
    		// Get list of remote vertices that were reached and updated. 
    		String logMsg = aStar(rootVertices, shortestDistanceMap, remoteUpdateSet);

    		log("END diskstras with subgraph local vertices="+ 
    				(subgraph.numVertices() - remoteVertexCount) + "," +logMsg);
    		
    		// Giraph:SimpleShortestPathsComputation.java:82
    		//     sendMessage(edge.getTargetVertexId(), new DoubleWritable(distance));

    		// Notify remote vertices of new known shortest distance from this subgraph and parent.
//    		for(Long remoteVertexID : remoteUpdateSet){
//    			String payload = remoteVertexID + "," + shortestDistanceMap.get(remoteVertexID).toString();
//    			SubGraphMessage msg = new SubGraphMessage(payload.getBytes());
//    			msg.setTargetSubgraph(subgraph.getVertex(remoteVertexID).getRemoteSubgraphId());
//    			sendMessage(msg);
//                changeCount++;
//    		}

    		// Aggregate messages to remote subgraph
    		changeCount = remoteUpdateSet.size();
    		messageCount = packAndSendMessages(remoteUpdateSet);
        }

		log("END superstep. Sent remote vertices = " + changeCount + ", remote messages =" + messageCount);

		// if no distances were changed, we terminate. 
        // if no one's distances change, everyone has votd to halt
//        if(changeCount == 0) {
			log("Voting to halt"); 
			// we're done
	        voteToHalt();         
//        }

        ///////////////////////////////////////////////
        /// Log the distance map
    	// FIXME: Charith, we need an finally() method later on
        if(verbosity > 0 || changeCount == 0) {
		    // print distance map and remote out messages
	        try {
	        	Path filepath = logRootDir.resolve("from-" + sourceVertexID + "-pt-" + partition.getId() + "-sg-"+subgraph.getId()+"-" + superStep + ".sssp");
				System.out.println("Writing mappings to file " + filepath);
	            File file = new File(filepath.toString());                    
	            PrintWriter writer = new PrintWriter(file);
	            writer.println("# Source vertex,"+sourceVertexID);
	            writer.println("## Sink vertex, Distance, Sink Parent");
	    		for(ITemplateVertex v : subgraph.vertices()){
	    			if(!v.isRemote()) { // print only non-remote vertices
	    				DistanceParentPair distanceParentPair = shortestDistanceMap.get(v.getId());
	    				if(distanceParentPair.distance != Short.MAX_VALUE) // print only connected vertices
	    					writer.println(v.getId() + "," + distanceParentPair.distance + "," + distanceParentPair.parent);
	    			}
	    		}
	            writer.flush();
	            writer.close();                                        
	        } catch (FileNotFoundException e) {
	            e.printStackTrace();
	        }            	
        }
    	}catch(RuntimeException ex){
    		if(logFileName == null) logFileName = "ERROR.log";
    		log("Unknown error in compute", ex);
    	}
    }



	private int packAndSendMessages(Set<Long> remoteUpdateSet){
    	
    	Map<Long, StringBuilder> remoteSubgraphMessageMap = new HashMap<>();
		for(Long remoteVertexID : remoteUpdateSet){
			long remoteSubgraphId = subgraph.getVertex(remoteVertexID).getRemoteSubgraphId();
			StringBuilder b = remoteSubgraphMessageMap.get(remoteSubgraphId);
			if(b == null) {
				b = new StringBuilder();
				remoteSubgraphMessageMap.put(remoteSubgraphId, b);
			}
			
			b.append(remoteVertexID).
				append(',').
				append(shortestDistanceMap.get(remoteVertexID).toString()).
				append(';');
		}

		// send outgoing messages to remote edges
		for (Map.Entry<Long, StringBuilder> entry : remoteSubgraphMessageMap.entrySet()) {
			SubGraphMessage message = new SubGraphMessage(entry.getValue().toString().getBytes());
			message.setTargetSubgraph(entry.getKey());
			sendMessage(message);
		}
		
		return remoteSubgraphMessageMap.size();
    }

    private List<String> unpackSubgraphMessages(
			List<SubGraphMessage> packedSubGraphMessages) {

    	List<String> remoteMessages = new ArrayList<String>();
    	for(SubGraphMessage message : packedSubGraphMessages){
    		String[] messages = new String(message.getData()).split(";");
    		remoteMessages.addAll(Arrays.asList(messages));
    	}
    	
		return remoteMessages;
	}


	/***
	 * Calculate (updated) distances and their parents based on traversals starting at "root"
	 * If remote vertices were reached, add them to remote update set and return.
	 * This is similar to the A* algorithm pattern. This method is not thread safe since
	 * the shortestDistanceMap and the remoteUpdateSet are modified. 
	 * The algorithm is run on the template by traversing from the rootVertices,
	 * and the edge weights are assumed to be 1.
	 *
	 * @param rootVertices the initial set of vertices that have external updates
	 * @param shortestDistanceMap a map from the list of vertices to their shortest known distance+parent. 
	 * 			This is passed as input and also updated by this method. 
	 * @param remoteUpdateSet a list of remote vertices whose parent distances have changed. 
	 * 			This is passed as input and also updated by this method.
	 */
    public static String aStar(
    		Set<ITemplateVertex> rootVertices,
    		Map<Long, DistanceParentPair> shortestDistanceMap, 
    		Set<Long> remoteUpdateSet){

    	// add root vertex whose distance was updated to the sorted distance list
    	// assert rootVertex.isRemote() == false
    	
    	// queue of vertices to traverse, sorted by shortest known distance
    	// We are simulating a ordered set using a hashmap (to test uniqueness) and priority queue (for ordering)
    	// Note that SortedSet does not allow comparable and equals to be inconsistent.
    	// i.e. we need equals to operate on vertex ID while comparator to operate on vertex distance
    	// NOTE: Maybe using TreeSet with Comparator passed in constructor may work better?
    	PriorityQueue<DistanceVertex> localUpdateQueue = new PriorityQueue<>();
    	Map<Long,DistanceVertex> localUpdateMap = new HashMap<>();
    	for(ITemplateVertex rootVertex : rootVertices) {
    		DistanceParentPair rootDistanceParentPair = shortestDistanceMap.get(rootVertex.getId());
    		DistanceVertex distanceVertex = new DistanceVertex(rootVertex, rootDistanceParentPair.distance);
    		localUpdateQueue.add(distanceVertex);
    		localUpdateMap.put(rootVertex.getId(), distanceVertex);
    	}
    	
    	
		ITemplateVertex currentVertex;
		DistanceVertex currentDistanceVertex;
		
		// FIXME:TEMPDEL: temporary variable for logging
		long localUpdateCount = 0, incrementalChangeCount = 0; 
		
		// pick the next vertex with shortest distance
		long count = 0;
		while((currentDistanceVertex = localUpdateQueue.poll()) != null) { // remove vertex from queue
			localUpdateMap.remove(currentDistanceVertex.vertex.getId()); // remote vertex from Map
			localUpdateCount++; // FIXME:TEMPDEL
			
			// get the shortest distance for the current vertex
			currentVertex = currentDistanceVertex.vertex;
			long currentVertexID = currentVertex.getId();
			short distanceToCurrent = currentDistanceVertex.distance;
			
			// BFS traverse to children of current vertex 
			// update their shortest distance if necessary
			// add them to update set if distance has changed
			for(ITemplateEdge e : currentVertex.outEdges()){
				
				// get child vertex
	    		ITemplateVertex childVertex = e.getSink(currentVertex);
	    		long childVertexID = childVertex.getId();
	    		boolean isChildVertexRemote = childVertex.isRemote();
	    		DistanceParentPair childDistanceParent = shortestDistanceMap.get(childVertexID);
	    		short childDistance = childDistanceParent.distance;

				// get the weight of the edge to childVertex
	    		// assume default edge weight is 1, unless a different value is given by the instance
	    		short edgeWeight = 1; 
	    		
	    		// calculate potential new distance for child
	    		short newChildDistance = (short)(distanceToCurrent + edgeWeight); // FIXME: this will not work for weighted edges
	    		
	    		// update distance to childVertex if it has improved
	    		if(childDistance > newChildDistance) {
	    			if(childDistance != Short.MAX_VALUE) incrementalChangeCount++;
	    				
	    			childDistanceParent.distance = newChildDistance;
	    			childDistanceParent.parent = currentVertexID;

	    			// if child is a remote vertex, then update its "local" shortest path.
	    			// note that we don't know what its global shortest path is.
		    		if(isChildVertexRemote) {
			    		// add to remote update set ...
		    			remoteUpdateSet.add(childVertexID);
		    		} else {
		    			// if child does not exist, add to queue and map
		    			if(!localUpdateMap.containsKey(childVertexID)){
		    	    		DistanceVertex childDistanceVertex = new DistanceVertex(childVertex, newChildDistance);
		    	    		localUpdateQueue.add(childDistanceVertex);
		    	    		localUpdateMap.put(childVertexID, childDistanceVertex);
		    				
		    			} else {
		    				// else update priority queue
		    				DistanceVertex childDistanceVertex = localUpdateMap.get(childVertexID);
		    				localUpdateQueue.remove(childDistanceVertex);
		    				childDistanceVertex.distance = newChildDistance;
		    				localUpdateQueue.add(childDistanceVertex);
		    			}
		    		}
	    		} // end if better path
			} // end edge traversal
			count++;

			// verbose
			if(verbosity > 0) {
				if((count % 100) == 0) System.out.print(".");
				if((count % 1000) == 0) System.out.println("@"+localUpdateQueue.size());
			}
			
		} // end vertex traversal
		
		// FIXME:TEMPDEL
		return "localUpdateCount=" + localUpdateCount + ", incrementalChangeCount="+incrementalChangeCount; // TEMPDEL
    }

	/**
	 * Log message to file
	 * @param message
	 */
	private void log(String message) {
//        try(PrintWriter writer = new PrintWriter(new FileOutputStream(logRootDir.resolve(logFileName).toFile(), true))){
//
//	        writer.println(System.currentTimeMillis()+":"+partitionId + ":" + subgraphId + ":" + superStep + ":" + message);
//	        writer.flush();
//
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
	}

	private void log(String message, Exception ex) {
//        try(PrintWriter writer = new PrintWriter(new FileOutputStream(logRootDir.resolve(logFileName).toFile(), true))){
//
//	        writer.println(System.currentTimeMillis()+":"+partitionId + ":" + subgraphId + ":" + superStep + ": ERROR! " + message);
//	        ex.printStackTrace(writer);
//	        writer.flush();
//
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
	}

}
