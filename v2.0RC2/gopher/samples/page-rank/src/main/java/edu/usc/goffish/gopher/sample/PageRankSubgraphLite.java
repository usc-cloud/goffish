package edu.usc.goffish.gopher.sample;

import java.io.*;
import java.nio.file.*;
import java.util.*;

import edu.usc.goffish.gofs.*;
import edu.usc.goffish.gopher.api.*;

/**
 * Yogesh: Updated to reduce hashmap puts, improved string perf
 */
public class PageRankSubgraphLite extends GopherSubGraph {

	private static final int PAGERANK_LOOPS = 1;
	
	private final class MyDouble {
		public double d;
	}
	
	public PageRankSubgraphLite() {
	}
	
	private Map<Long, Double> _weights;	
	private Map<Long, MyDouble> sums;
	
	@Override
	public void compute(List<SubGraphMessage> messages) {
		if (superStep == 0) {
			_weights = new HashMap<>(subgraph.numVertices(), 1f);
			sums = new HashMap<>(subgraph.numVertices(), 1f);

			// initialize weights
			// initialize sums
			for (ITemplateVertex vertex : subgraph.vertices()) {
				_weights.put(vertex.getId(), 1D);
				sums.put(vertex.getId(), new MyDouble());
			}
		}

		//System.out.println("weights initialized");
		
		MyDouble myD;
		for (int i = 0; i < PAGERANK_LOOPS; i++)
		{
			//System.out.println("pagerank loop " + i);
			
			
			// calculate sums from this subgraphs
			for (ITemplateVertex vertex : subgraph.vertices()) {
				if (vertex.isRemote()) {
					// weights from remote vertices are sent through messages
					continue;
				}

				double delta = _weights.get(vertex.getId()) / vertex.outDegree();
				for (ITemplateEdge edge : vertex.outEdges()) {
					myD = sums.get(edge.getSink(vertex).getId());					//sinkVertexId
					myD.d += delta;
				}
			}
			
			//System.out.println("sums calculated");
			
			// add in sums from remote vertices
			for (SubGraphMessage message : messages) {
				// fixme: use inline string.split for even better perf 
				// use ":" as global separator for better perf. O(s) complexity, where s is string length
				String[] parts = new String(message.getData()).split(":"); 
				for (i=0; i<parts.length;i++) { //String part : parts
					long vertexId = Long.parseLong(parts[i]);
					i++;
					double delta = Double.parseDouble(parts[i]);
					myD = sums.get(vertexId);
					myD.d += delta;
				}
			}
			
			//System.out.println(messages.size() + " messages recieved");
			
			// update weights
			for (ITemplateVertex vertex : subgraph.vertices()) {
				if (vertex.isRemote()) {
					continue;
				}
				myD = sums.get(vertex.getId());
				double pr = 0.15 + 0.85 * myD.d;
				_weights.put(vertex.getId(), pr);
				// set sum of non-remote vertices to zero here to avoid doing it at start of next superstep
				myD.d = 0d;
			}
		}
		
		if (superStep < 30) {
			
			// message aggregation
			HashMap<Long, StringBuilder> messageAggregator = new HashMap<>(subgraph.numRemoteVertices(), 1f);
			for (ITemplateVertex remoteVertex : subgraph.remoteVertices()) {
				StringBuilder b = messageAggregator.get(remoteVertex.getRemoteSubgraphId());
				if (b == null) {
					b = new StringBuilder();
					messageAggregator.put(remoteVertex.getRemoteSubgraphId(), b);
				}
				
				myD = sums.get(remoteVertex.getId());
				b.append(remoteVertex.getId()).append(':').append(myD.d).append(':');
				// set sum of remote vertices to zero here to avoid doing it at start of next superstep
				myD.d = 0d;
			}
			
			// send outgoing weights to remote edges
			for (Map.Entry<Long, StringBuilder> entry : messageAggregator.entrySet()) {
				SubGraphMessage message = new SubGraphMessage(entry.getValue().toString().getBytes());
				message.setTargetSubgraph(entry.getKey());
				sendMessage(message);
			}

			//System.out.println("messages sent");
		} else {
			voteToHalt();
			System.out.println("voting to halt");
			
			try(BufferedWriter output = new BufferedWriter(new OutputStreamWriter(Files.newOutputStream(Paths.get("ss_" + superStep + "_subgraph_" + subgraph.getId() + "_output"))))) {
				for (ITemplateVertex vertex : subgraph.vertices()) {
					if (!vertex.isRemote()) {
						output.write(vertex.getId() + " " + _weights.get(vertex.getId()) + System.lineSeparator());
					}
				}
			} catch (IOException e) {
				System.out.println(e);
			}
		}
	}

	// http://hg.openjdk.java.net/jdk7/jdk7/jdk/rev/1ff977b938e5
	public static final String split(String source, char ch){
		int off = 0;
		int next = 0;
		while ((next = source.indexOf(ch, off)) != -1) {
			return source.substring(off, next);
			//off = next + 1;
		}
		return null;
	}
}
