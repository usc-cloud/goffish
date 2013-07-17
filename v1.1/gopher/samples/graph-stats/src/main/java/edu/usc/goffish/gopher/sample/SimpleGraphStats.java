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

import edu.usc.goffish.gofs.INameNode;
import edu.usc.goffish.gofs.IPartition;
import edu.usc.goffish.gofs.ISubgraph;
import edu.usc.goffish.gofs.TemplateEdge;
import edu.usc.goffish.gofs.TemplateVertex;
import edu.usc.goffish.gofs.graph.Edge;
import edu.usc.goffish.gofs.namenode.FileNameNode;
import edu.usc.goffish.gofs.slice.FileStorageManager;
import edu.usc.goffish.gofs.slice.JavaSliceSerializer;
import edu.usc.goffish.gofs.slice.SliceManager;
import edu.usc.goffish.gopher.api.GopherSubGraph;
import edu.usc.goffish.gopher.api.SubGraphMessage;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;

/***
 * This sample calculates the total number of vertices and edges, number of subgraphs, 
 * mean/min/max/histogram of vertices/local|remote-edges per subgraph, histogram of edge degree
 * 
 * TODO: diameter, radius, WCC, etc.
 * TODO: # of attributes, directed, etc.
 * TODO: TS specific stats
 * 
 * 
 * Step 1: Calculate local subgraph stats. Send to Min subgraph ID in local Partition. Everyone but min subgraph IDs votetohalt().
 * Step 2: Calculate partition stats. Send to Min subgraph ID in min partition ID. Everyone but min partition ID votetohalt().
 * Step 3: Write stats to file. Everyone votetohalt().
 * 
 * @author simmhan
 *
 */
public class SimpleGraphStats extends GopherSubGraph {

	/***
	 * 0 == do not write anything to file
	 * 1 == write graph level details to file (by smallest partition)
	 * 2 == write partition-level details to file (one per partition)
	 * 3 == write sub-graph level details to file (one per subgraph)
	 * All files have .hist as extension.
	 */
    private static int verbosity = 3;
    private static Path logRootDir = Paths.get("."); // location where log files are saved

    private static final List<Long> vertexBoundaries = 
    		new ArrayList<Long>(Arrays.asList(new Long[]{0L, 1L, 10l, 100l, 1000l, 10000l, 100000l, 1000000l, 10000000l}));
    private static final List<Long> edgeBoundaries = 
    		new ArrayList<Long>(Arrays.asList(new Long[]{0L, 1L, 10l, 100l, 1000l, 10000l, 100000l, 1000000l, 10000000l, 100000000l}));
    private static final List<Long> degreeBoundaries = 
    		new ArrayList<Long>(Arrays.asList(new Long[]{-1L, 0L, 1L, 5L, 10l, 20l, 50l, 100l, 1000l, 10000l}));
    private static final List<Long> sgCountBoundaries = 
    		new ArrayList<Long>(Arrays.asList(new Long[]{0L, 1L, 5L, 10l, 20l, 50l, 100l, 1000l, 10000l, 100000l, 1000000l}));

    /***
     * Gets/writes stats about subgraph
     * 
     * @param partition
     * @param subgraph
     * @return
     */
    public static String getSubgraphStats(IPartition partition,  ISubgraph subgraph){
		// This sample calculates the total number of vertices and edges, number of subgraphs, 
		// mean/min/max/histogram of vertices/local|remote-edges per subgraph, histogram of edge degree
		// subgraphs per partition
		int sgVertexCount = subgraph.getTemplate().numVertices(); // FIXME: should we subtract "remote" vertices?
		long sgEdgeCount = subgraph.getTemplate().numEdges(); // FIXME: should we subtract "remote" edges?
		
	    Histogram sgEdgeDegreeHistogram = new Histogram(degreeBoundaries, true);
		long sgRemoteVertexCount = 0; // vertices that are on a different partition
		
		Map<Long, Long> inDegreeMap = new HashMap<Long, Long>();
        for(TemplateVertex v : subgraph.getTemplate().vertices()){
        	int degree = v.outDegree();
        	// maintain in degree stats for directed graph // fixme: support in graph API
        	if(subgraph.isDirected()) {
	        	for(TemplateEdge e : v.outEdges()){
	        		long sinkV = e.getSink().getId();
	        		if(!inDegreeMap.containsKey(sinkV)) inDegreeMap.put(sinkV, 0L);
	        		inDegreeMap.put(sinkV, inDegreeMap.get(sinkV)+1); // increment in degree of vertex
	        	}
        	}
        	
        	sgEdgeDegreeHistogram.add(degree);
        	// this is the # of remote vertices on a different partition
        	// since by definition of the subgraph, there cannot be an edge between two subgraph within a partition
        	if(v.isRemote()) sgRemoteVertexCount++;	
        }

	    Histogram sgEdgeInDegreeHistogram = new Histogram(degreeBoundaries, true);
    	if(subgraph.isDirected()) {
	        for(long inDegree : inDegreeMap.values()){
	        	sgEdgeInDegreeHistogram.add(inDegree);
	        }
    	}
    	
        // print subgraph details
        if(verbosity >=3) {
        	StringBuffer sb = new StringBuffer();
        	sb.append("Partition\t").append(partition.getId()).append('\n');
        	sb.append("Subgraph\t").append(subgraph.getId()).append('\n');
        	
        	// add subgraph info
        	sb.append("Vertex Count\t").append(sgVertexCount).append('\n');
        	sb.append("Edge Count\t").append(sgEdgeCount).append('\n');
        	sb.append("Remote Vertex Count\t").append(sgRemoteVertexCount).append('\n');
        	double avgEdgeDegree = sgEdgeDegreeHistogram.getCount() <= 0 ? 0 :
        		((double)sgEdgeDegreeHistogram.getSum())/sgEdgeDegreeHistogram.getCount();
        	sb.append("Avg Edge (Out) Degree\t").append(avgEdgeDegree).append('\n');
        	
        	if(subgraph.isDirected()) {
	        	double avgInEdgeDegree = sgEdgeInDegreeHistogram.getCount() <= 0 ? 0 :
	        		((double)sgEdgeInDegreeHistogram.getSum())/sgEdgeInDegreeHistogram.getCount();
	        	sb.append("Avg Edge In Degree\t").append(avgInEdgeDegree).append('\n');
        	}
        	
        	sb.append("Edge (Out) Degree for Vertices Histogram...\n");
        	sgEdgeDegreeHistogram.prettyPrint(sb);                	
        	
        	if(subgraph.isDirected()) {
            	sb.append("Edge In Degree for Vertices Histogram...\n");
            	sgEdgeInDegreeHistogram.prettyPrint(sb);            	
        	}
        	
            try {
            	Path filepath = logRootDir.resolve("sg-"+partition.getId()+"-"+subgraph.getId()+".hist");
                File file = new File(filepath.toString());                    
                PrintWriter writer = new PrintWriter(file);
                writer.write(sb.toString());
                writer.flush();
                writer.close();
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }            	
        }
        
        // serialize: edge degree histogram, myRemoteVertexCount, myVertexCount, myEdgeCount
        StringBuffer message = new StringBuffer();
        message.append(partition.getId()).append('#');
        message.append(subgraph.getId()).append('#');

        message.append(sgVertexCount).append('#');
        message.append(sgEdgeCount).append('#');
        message.append(sgRemoteVertexCount).append('#');
        message.append(sgEdgeDegreeHistogram.saveToString());

        // fixme: add edge in degree?
        
        return message.toString();
    }

    public static String getPartitionStats(IPartition partition,  ISubgraph subgraph, List<String> subGraphMessages){
        
//This sample calculates the total number of vertices and edges, number of subgraphs, 
//mean/min/max/histogram of vertices/local|remote-edges per subgraph, histogram of edge degree
    	    Histogram sgVerticesHistogram = new Histogram(vertexBoundaries, true);
    	    Histogram sgEdgesHistogram = new Histogram(edgeBoundaries, true);
    	    Histogram sgRemoteVerticesHistogram = new Histogram(edgeBoundaries, true); // this is like subgraph "degree"
    	    Histogram partEdgeDegreeHistogram = new Histogram(degreeBoundaries, true);
    	    
            for(String message : subGraphMessages) {

                // extract partition, subgraph, and payload
                String[] splits = message.split("#");

                // 0: partition
                // 1: subgraph
                // 2: sgVertexCount
                // 3: sgEdgeCount
                // 4: sgRemoteVertexCount
                // 5: sgEdgeDegreeHistogram
                int i = 0;
                long sgPartitionId = Long.parseLong(splits[i]);
                i++;
                assert sgPartitionId == partition.getId();
                
                long sgId = Long.parseLong(splits[i]);
                i++;
                String sgIdStr = ""+sgId;
                
                long sgVertexCount= Long.parseLong(splits[i]);
                i++;
                sgVerticesHistogram.add(sgVertexCount, sgIdStr);
                
                long sgEdgeCount= Long.parseLong(splits[i]);
                i++;
                sgEdgesHistogram.add(sgEdgeCount, sgIdStr);
                
                long sgRemoteVertexCount= Long.parseLong(splits[i]);
                i++;
                sgRemoteVerticesHistogram.add(sgRemoteVertexCount, sgIdStr);
                
                Histogram sgEdgeDegreeHistogram = new Histogram(splits[i]);
                partEdgeDegreeHistogram.merge(sgEdgeDegreeHistogram);
                
                // fixme: add edge in degree?
            }

    	    // number of messages received == number of subgraphs in this partition
    		int sgCount = subGraphMessages.size();

            // we can get these values from the histogram data
    		long partVertexCount = sgVerticesHistogram.getSum();
    		long partEdgeCount = sgEdgesHistogram.getSum();
    		
    		// NOTE: this may capture duplicate vertices. i.e. the same remote vertex from multiple subgraphs may be repetitively counted 
    		// So this does not really give us the # of edge cuts
    		long partRemoteVertexCount = sgRemoteVerticesHistogram.getSum();

        	double avgEdgeDegree = partEdgeDegreeHistogram.getCount() <= 0 ? 0 :
        		partEdgeDegreeHistogram.getSum()/partEdgeDegreeHistogram.getCount();
    		
            // fixme: add edge in degree?

            // print partition details
            if(verbosity >=2) {
                	StringBuffer sb = new StringBuffer();
                	sb.append("Partition\t").append(partition.getId()).append('\n');
                	
                	// add subgraph info
                	sb.append("Subgraph Count\t").append(sgCount).append('\n');
                	sb.append("Vertex Count\t").append(partVertexCount).append('\n');
                	sb.append("Edge Count\t").append(partEdgeCount).append('\n');
                	sb.append("Remote Vertex Count\t").append(partRemoteVertexCount).append('\n');
                	sb.append("(NOTE: Vertex Counts may include duplicate counts from different local subgraphs to same remote vertex)\n");
                	sb.append("Avg Edge Degree\t").append(avgEdgeDegree).append("\n\n");
                	
                	sb.append("Vertex Count for Subgraphs Histogram...\n");
                	sgVerticesHistogram.prettyPrint(sb);             	
                	sb.append("Edge Count for Subgraphs Histogram...\n");
                	sgEdgesHistogram.prettyPrint(sb);                	
                	sb.append("Remote Vertex Count for Subgraphs Histogram...\n");
                	sgRemoteVerticesHistogram.prettyPrint(sb);                	
                	sb.append("Edge Degree for Subgraphs/Vertices Histogram...\n");
                	partEdgeDegreeHistogram.prettyPrint(sb);                	
                	
                try {
                	Path filepath = logRootDir.resolve("pt-"+partition.getId()+".hist");
                    File file = new File(filepath.toString());                    
                    PrintWriter writer = new PrintWriter(file);
                    writer.write(sb.toString());
                    writer.flush();
                    writer.close();
                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                }            	
            }           
            // serialize: edge degree histogram, myRemoteVertexCount, myVertexCount, myEdgeCount
            StringBuffer message = new StringBuffer();
            message.append(partition.getId()).append('#');
    
            message.append(sgCount).append('#');
            message.append(sgVerticesHistogram.saveToString()).append('#');
            message.append(sgEdgesHistogram.saveToString()).append('#');
            message.append(sgRemoteVerticesHistogram.saveToString()).append('#');
            message.append(partEdgeDegreeHistogram.saveToString());
            
            return message.toString();            
    }
    
    
    @Override
    public void compute(List<SubGraphMessage> subGraphMessages) {
        
    	///////////////////////////////////////////////////////////
    	// First superstep. calculate subgraph stats.
    	if(superStep == 0) { // fixme: should this be 1?
    		
    		String message = getSubgraphStats(partition, subgraph);
            
            // send message to my current partition for "aggregation" (broadcasts to all subgraphs!)
            // FIXME: send to smallest subgraph ID in my partition once future API supports            
            SubGraphMessage<String> msg = new SubGraphMessage<String>(message.getBytes());
            sentMessage(partition.getId(), msg);
            
            return;
        } 
    	
    	
    	///////////////////////////////////////////////////////////
    	// second superstep. Collect partition stats by aggregating local subgraph stats.
    	if(superStep == 1) {

    		// FIXME: Ideally, the message(s) would have been received by only the "smallest" subgraph ID 
    		// FIXME: Since it was broadcast to all sugraphs, use "file create" as way to pick one that does agregation
    		// Create file atomically. Only one subgraph will "win" for this partition.
    		boolean isAggregator;
    		
    		Path filepath = logRootDir.resolve("pt-"+partition.getId()+".hist");
    		try {
    			Files.createFile(filepath);
    			// if this succeeds, then this subgraph will perform agg for the partition
    			isAggregator = true; 
    		} catch(FileAlreadyExistsException faeex){
    			isAggregator = false; // expected
    		} catch (IOException e) {
    			e.printStackTrace();
    			isAggregator = false;
    		}

    		// halt, if not the smallest partition
            int smallestPartitionId = Collections.min(partitions);

            // assert that only one subgraph has isAggregator as true
    		if(isAggregator) {

    			List<String> messages = new ArrayList<String>();
    			for(SubGraphMessage<String> msg : subGraphMessages) {
    				messages.add(new String(msg.getData()));    				
    			}
    			
    			String message = getPartitionStats(partition, subgraph, messages);
	            // send aggregate message to smallest partition in graph
	            SubGraphMessage<String> msg = new SubGraphMessage<String>(message.toString().getBytes());
	            sentMessage(smallestPartitionId, msg);
    		}
    		
    		
            // if not the smallest partition, vote to halt
    		if(partition.getId() != smallestPartitionId)
    			voteToHalt();
        }
    	
    	// third superstep. Collect graph stats.
    	if(superStep >= 2) {
    		
    		// halt, if not the smallest partition
            int smallestPartitionId = Collections.min(partitions);
    		boolean isAggregator = (partition.getId() == smallestPartitionId);

    		// assert that only one partition has isAggregator as true
    		if(isAggregator) {
    			
	    	    Histogram partVerticesHistogram = new Histogram(vertexBoundaries, true);
	    	    Histogram partEdgesHistogram = new Histogram(edgeBoundaries, true);
	    	    Histogram partRemoteVerticesHistogram = new Histogram(edgeBoundaries, true); // this is like subgraph "degree"
	    	    Histogram partSGCountHistogram = new Histogram(sgCountBoundaries, true); 
	    	    Histogram graphEdgeDegreeHistogram = new Histogram(degreeBoundaries, true);
	    	    
	            for(SubGraphMessage<String> msg : subGraphMessages) {
	
	                String message = new String(msg.getData());
	                // extract partition, subgraph, and payload
	                String[] splits = message.split("#");

		            // 0: partition
	                // 1: sgCount 
	                // 2: sgVertexCount histogram
	                // 3: sgEdgeCount histogram
	                // 4: sgRemoteVertexCount histogram
	                // 5: partEdgeDegreeHistogram histogram
	                int i = 0;
	                long partitionId = Long.parseLong(splits[i]);
	                i++;
	                String partIdStr = ""+partitionId;
	                
	                long partSGCount= Long.parseLong(splits[i]);
	                i++;
	                partSGCountHistogram.add(partSGCount, partIdStr);

	                Histogram sgVerticesHistogram = new Histogram(splits[i]);
	                i++;
	                partVerticesHistogram.merge(sgVerticesHistogram);
	                
	                Histogram sgEdgesHistogram = new Histogram(splits[i]);
	                i++;
	                partEdgesHistogram.merge(sgEdgesHistogram );	               
	                
	                Histogram sgRemoteEdgesHistogram = new Histogram(splits[i]);
	                i++;
	                partRemoteVerticesHistogram.merge(sgRemoteEdgesHistogram);
	                
	                Histogram partEdgeDegreeHistogram = new Histogram(splits[i]);
	                graphEdgeDegreeHistogram.merge(partEdgeDegreeHistogram);
	            }

	    	    // number of messages received == number of partitions in this graph
	    		int partCount = subGraphMessages.size();

	            // we can get these values from the histogram data
	    		long sgCount = partSGCountHistogram.getSum();
	    		// NOTE: these may capture duplicate vertices and edges. i.e. the same remote vertex/edge from multiple subgraphs may be repetitively counted 
	    		long graphVertexCount = partVerticesHistogram.getSum();
	    		long graphEdgeCount = partEdgesHistogram.getSum();	    		
	    		long graphRemoteVertexCount = partRemoteVerticesHistogram.getSum();
	    		
	    		double avgEdgeDegree = graphEdgeDegreeHistogram.getCount() <= 0 ? 0 :
            		graphEdgeDegreeHistogram.getSum()/graphEdgeDegreeHistogram.getCount();

	            // print partition details
	            if(verbosity >=1) {
                	StringBuffer sb = new StringBuffer();
                	sb.append("Graph!\n");
                	
                	// add subgraph info
                	sb.append("Partition Count\t").append(partCount).append('\n');
                	sb.append("Subgraph Count\t").append(sgCount).append('\n');
                	sb.append("Vertex Count\t").append(graphVertexCount).append('\n');
                	sb.append("Edge Count\t").append(graphEdgeCount).append('\n');
                	sb.append("Remote Vertex Count\t").append(graphRemoteVertexCount).append('\n');
                	sb.append("(NOTE: Vertex/Edge Counts may include duplicate counts from different local subgraphs/partitions)\n");
                	sb.append("Avg Edge Degree\t").append(avgEdgeDegree).append("\n\n");
                	
    	    	    sb.append("Subgraph Count for Partitions Histogram...\n");
    	    	    partSGCountHistogram.prettyPrint(sb);             	
    	    	    sb.append("Vertex Count for Subgraphs Histogram...\n");
    	    	    partVerticesHistogram.prettyPrint(sb);             	
                	sb.append("Edge Count for Subgraphs Histogram...\n");
                	partEdgesHistogram.prettyPrint(sb);                	
                	sb.append("Remote Vertex Count for Subgraphs Histogram...\n");
                	partRemoteVerticesHistogram.prettyPrint(sb);                	
                	sb.append("Edge Degree for Subgraphs/Vertices Histogram...\n");
                	graphEdgeDegreeHistogram.prettyPrint(sb);                	
                	try{
	            		Path filepath = logRootDir.resolve("graph-" + System.currentTimeMillis() + ".hist");    		
	                    File file = new File(filepath.toString());                    
	                    PrintWriter writer = new PrintWriter(file);
	                    writer.write(sb.toString());
	                    writer.flush();
	                    writer.close();
	                } catch (FileNotFoundException e) {
	                    e.printStackTrace();
	                }            	
	            }
    		}		
    	}

    	// everyone votes to halt
        voteToHalt();        
    }
    
    
    public static void main(String[] args){
        // Creating slice manager
        try {
        	if(args.length < 3) {
        		System.out.println("USAGE: SimpleGraphStats <name node file path> <slice file path> <graph id> [<partition messages file>]");
        		return;
        	}
        	
            String nameNodeFile = args[0]; 
            String sliceFilePath = args[1]; 
            String gid = args[2];

            INameNode nameNode = new FileNameNode(Paths.get(nameNodeFile));
            
            ArrayList<Integer> partList = new ArrayList<Integer>();
            partList.addAll(nameNode.getPartitions(gid));

            URI currentPartURI = null;

            for(int pid : partList) {
                URI uri  = nameNode.getPartitionMapping(gid,pid);
                System.out.println(uri);
                if(uri.getPath().contains(sliceFilePath)) {
                    currentPartURI = uri;
                }
            }

	    String fragment = currentPartURI.getFragment();
	System.out.println(fragment);
	System.out.println(Paths.get(sliceFilePath));

            SliceManager mySliceManager = new SliceManager(UUID.fromString(currentPartURI.getFragment()), new JavaSliceSerializer(),
                    new FileStorageManager(Paths.get(sliceFilePath)));


            IPartition myPartition = mySliceManager.readPartition();

            List<String> messages = new ArrayList<String>();
            ISubgraph lastSubgraph = null; // acts as aggregator
            // superstep 0
            for(ISubgraph mySubgraph : myPartition) {
		System.out.println("SG ID:" + mySubgraph.getId());
            	String message = getSubgraphStats(myPartition, mySubgraph);
            	messages.add(message);
            	lastSubgraph = mySubgraph;
            }
            
            // superstep 1
            if(lastSubgraph != null) {
            	String message = getPartitionStats(myPartition, lastSubgraph, messages);

                try {
                	Path filepath = logRootDir.resolve("pt-"+myPartition.getId()+".msgs");
                    File file = new File(filepath.toString());                    
                    PrintWriter writer = new PrintWriter(file);
                    writer.write(message);
                    writer.flush();
                    writer.close();
                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                }            	
            }
                        
            
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
