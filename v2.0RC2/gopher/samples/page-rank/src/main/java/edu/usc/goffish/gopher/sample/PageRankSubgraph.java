package edu.usc.goffish.gopher.sample;

import it.unimi.dsi.fastutil.longs.*;

import java.io.*;
import java.nio.file.*;
import java.util.*;

import edu.usc.goffish.gofs.*;
import edu.usc.goffish.gopher.api.*;

public class PageRankSubgraph extends GopherSubGraph {
	
	private static final double D = 0.85D;
	private static final double PAGERANK_EPSILON = .001D;
	
	private Long2DoubleMap _localPageRanks;
	private Long2DoubleMap _sums;
	
	private int _numSubgraphs;
	private int _numVertices;
	
	private boolean _pageRankAggregateReady;
	private int _numPageRankFinishes;
	
	private void initialize() {
		_localPageRanks = new Long2DoubleOpenHashMap(subgraph.numVertices(), 1f);
		_sums = new Long2DoubleOpenHashMap(subgraph.numVertices(), 1f);
		
		_numSubgraphs = 0;
		_numVertices = 0;
		
		_pageRankAggregateReady = false;
		_numPageRankFinishes = 0;
		
		// send messages for # of subgraphs and # of vertices
		SubGraphMessage s1 = new SubGraphMessage(Message.subgraphMessage().toBytes());
		SubGraphMessage s2 = new SubGraphMessage(Message.numVerticesMessage(subgraph.numVertices() - subgraph.numRemoteVertices()).toBytes());
		for (int partitionId : partitions) {
			sendMessage(partitionId, s1);
			sendMessage(partitionId, s2);
		}
	}
	
	@Override
	public void compute(List<SubGraphMessage> stuff) {
		_numPageRankFinishes = 0;
		
		List<Message> messages = decode(stuff);
		
		if (superStep == 0) {
			initialize();
		} else if (superStep == 1) {
			// set initial values
			for (ITemplateVertex vertex : subgraph.vertices()) {
				if (!vertex.isRemote()) {
					_localPageRanks.put(vertex.getId(), 1.0D / _numVertices);
				}
			}
			
			// send out initial messages
			doPageRankIteration(Collections.<Message>emptyList(), false, true);
		} else {
			if (_pageRankAggregateReady) {
				if (_numPageRankFinishes == _numSubgraphs) {
					voteToHalt();
					outputToFile();
				}
			}
				
			if (!isVoteToHalt()) {
				double L1norm = doPageRankIteration(messages, true, true);
				
				if (L1norm < PAGERANK_EPSILON / _numSubgraphs) {
					// notify everyone that we're ok to finish
					SubGraphMessage s = new SubGraphMessage(Message.finishPageRankMessage().toBytes());
					for (int partitionId : partitions) {
						sendMessage(partitionId, s);
					}
				}
				
				_pageRankAggregateReady = true;
				
				outputSums();
			}
		}
	}

	private void outputToFile() {
		// output results
		try(BufferedWriter output = new BufferedWriter(new OutputStreamWriter(Files.newOutputStream(Paths.get("ss_" + superStep + "_subgraph_" + subgraph.getId() + "_output"))))) {
			for (ITemplateVertex vertex : subgraph.vertices()) {
				if (!vertex.isRemote()) {
					output.write(vertex.getId() + " " + _localPageRanks.get(vertex.getId()) + System.lineSeparator());
				}
			}
		} catch (IOException e) {
			System.out.println(e);
		}
	}
	
	private void outputSums() {
		double sum = 0;
		for (double value : _localPageRanks.values()) {
			sum += value;
		}
		
		// output results
		try(BufferedWriter output = new BufferedWriter(new OutputStreamWriter(Files.newOutputStream(Paths.get("ss_" + superStep + "_sum_subgraph_" + subgraph.getId() + "_output"))))) {
			output.write(Double.toString(sum) + System.lineSeparator());
		} catch (IOException e) {
			System.out.println(e);
		}
	}

	private double doPageRankIteration(Iterable<Message> messages, boolean updateValues, boolean sendMessages) {
		double L1norm = 0;
		
		if (updateValues) {
			// reset intermediate sums
			for (ITemplateVertex vertex : subgraph.vertices()) {
				if (!vertex.isRemote()) {
					_sums.put(vertex.getId(), 0.0D);
				}
			}
		}
		
		// calculate sums from this subgraphs
		for (ITemplateVertex vertex : subgraph.vertices()) {
			if (vertex.isRemote()) {
				continue;
			}
			
			double sentWeight = _localPageRanks.get(vertex.getId()) / vertex.outDegree();
			
			// sum local page ranks
			for (ITemplateEdge edge : vertex.outEdges()) {
				ITemplateVertex sink = edge.getSink(vertex);
				
				if (!sink.isRemote() && updateValues) {
					_sums.put(sink.getId(), _sums.get(sink.getId()) + sentWeight);
				} else if (sink.isRemote() && sendMessages) {
					Message m = new Message(sink.getId(), sentWeight);
					SubGraphMessage s = new SubGraphMessage(m.toBytes());
					s.setTargetSubgraph(sink.getRemoteSubgraphId());
					sendMessage(s);
				}
			}
		}

		if (updateValues) {
			// sum remote page ranks
			for (Message message : messages) {
				_sums.put(message.LocalVertexId, _sums.get(message.LocalVertexId) + message.SentPageRank);
			}
		
			// update weights
			for (ITemplateVertex vertex : subgraph.vertices()) {
				if (vertex.isRemote()) {
					continue;
				}
				
				double oldPageRank = _localPageRanks.get(vertex.getId());
				double newPageRank = (1 - D)/_numVertices + D * _sums.get(vertex.getId());
				_localPageRanks.put(vertex.getId(), newPageRank);
				L1norm += Math.abs(newPageRank - oldPageRank);
			}
		}
		
		return L1norm;
	}
	
	private List<Message> decode(List<SubGraphMessage> stuff) {
		if (stuff.isEmpty()) {
			return Collections.emptyList();
		}
		
		ArrayList<Message> messages = new ArrayList<>(stuff.size());
		for (SubGraphMessage s : stuff) {
			Message m = Message.fromBytes(s.getData());
			if (m.SubgraphMessage) {
				_numSubgraphs++;
			} else if (m.FinishPageRank) {
				_numPageRankFinishes++;
			} else if (m.NumVertices > 0) {
				_numVertices += m.NumVertices;
			} else {
				messages.add(m);
			}
		}
		
		return messages;
	}
	
	static class Message {
		
		final boolean SubgraphMessage;
		final boolean FinishPageRank;
		final int NumVertices;
		final long LocalVertexId;
		final double SentPageRank;
		
		private Message(boolean subgraphMessage, boolean finishPageRank) {
			SubgraphMessage = subgraphMessage;
			FinishPageRank = finishPageRank;
			LocalVertexId = Long.MIN_VALUE;
			SentPageRank = 0;
			NumVertices = 0;
		}
		
		static Message subgraphMessage() {
			return new Message(true, false);
		}
		
		static Message finishPageRankMessage() {
			return new Message(false, true);
		}
		
		private Message(int numVertices) {
			LocalVertexId = Long.MIN_VALUE;
			FinishPageRank = false;
			SentPageRank = 0;
			NumVertices = numVertices;
			SubgraphMessage = false;
		}
		
		static Message numVerticesMessage(int numVertices) {
			return new Message(numVertices);
		}
		
		Message(double sentPageRank) {
			LocalVertexId = Long.MIN_VALUE;
			FinishPageRank = false;
			SentPageRank = sentPageRank;
			NumVertices = 0;
			SubgraphMessage = false;
		}
		
		Message(long localVertexId, double sentPageRank) {
			LocalVertexId = localVertexId;
			FinishPageRank = false;
			SentPageRank = sentPageRank;
			NumVertices = 0;
			SubgraphMessage = false;
		}
		
		private Message(boolean subgraphMessage, boolean finishPageRank, int numVertices, long localVertexId, double sentPageRank) {
			SubgraphMessage = subgraphMessage;
			FinishPageRank = finishPageRank;
			NumVertices = numVertices;
			LocalVertexId = localVertexId;
			SentPageRank = sentPageRank;
		}
		
		byte[] toBytes() {
			return (Boolean.toString(SubgraphMessage) + "," + Boolean.toString(FinishPageRank) + "," + Integer.toString(NumVertices) + "," + Long.toString(LocalVertexId) + "," + Double.toString(SentPageRank)).getBytes();
		}
		
		static Message fromBytes(byte[] bytes) {
			String[] s = new String(bytes).split(",");
			return new Message(Boolean.parseBoolean(s[0]), Boolean.parseBoolean(s[1]), Integer.parseInt(s[2]), Long.parseLong(s[3]), Double.parseDouble(s[4]));
		}
	}
}
