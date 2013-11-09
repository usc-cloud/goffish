package edu.usc.goffish.gopher.sample;

import java.io.*;
import java.nio.file.*;
import java.util.*;

import edu.usc.goffish.gofs.*;
import edu.usc.goffish.gopher.api.*;

public class BlockRankSubgraph extends GopherSubGraph {
	
	private static final double D = 0.85D;
	private static final double LOCALPAGERANK_EPSILON = .1D;
	private static final double BLOCKRANK_EPSILON = .001D;
	private static final double PAGERANK_EPSILON = .001D;
	
	private Map<Long, Double> _localPageRanks;
	private Map<Long, Double> _intermediateLocalSums;
	
	private double _blockPageRank;
	private int _numBlockOutEdges;
	private int _numSubgraphs;
	private int _numVertices;
	
	private boolean _inBlockRankMode;
	private boolean _firstBlockRankStep;
	private boolean _blockRankAggregateReady;
	private boolean _firstPageRankStep;
	private boolean _pageRankAggregateReady;
	
	private int _numBlockRankFinishes;
	private int _numPageRankFinishes;
	
	private void initialize() {
		_localPageRanks = new HashMap<>(subgraph.numVertices(), 1f);
		_intermediateLocalSums = new HashMap<>(subgraph.numVertices(), 1f);
		
		for (ITemplateVertex vertex : subgraph.vertices()) {
			if (vertex.isRemote()) {
				continue;
			}
			
			_localPageRanks.put(vertex.getId(), 1.0D / (subgraph.numVertices() - subgraph.numRemoteVertices()));
		}
		
		_numBlockOutEdges = subgraph.numRemoteVertices();
		_numSubgraphs = 0;
		_numVertices = 0;
		
		_inBlockRankMode = true;
		_firstBlockRankStep = true;
		_blockRankAggregateReady = false;
		_firstPageRankStep = true;
		_pageRankAggregateReady = false;
		
		SubGraphMessage s1 = new SubGraphMessage(Message.subgraphMessage().toBytes());
		SubGraphMessage s2 = new SubGraphMessage(Message.numVerticesMessage(subgraph.numVertices() - subgraph.numRemoteVertices()).toBytes());
		for (int partitionId : partitions) {
			sendMessage(partitionId, s1);
			sendMessage(partitionId, s2);
		}
	}
	
	@Override
	public void compute(List<SubGraphMessage> stuff) {
		
		_numBlockRankFinishes = 0;
		_numPageRankFinishes = 0;
		
		List<Message> messages = decode(stuff);
		
		if (superStep == 0) {
			initialize();
			
			// calculate local page ranks
			double e;
			do {
				e = doPageRankIteration(true, Collections.<Message>emptyList(), false);
			} while(e >= LOCALPAGERANK_EPSILON);
			
			return;
		} else if (superStep == 1) {
			_blockPageRank = 1.0D / _numSubgraphs;
		}
		
		if (_inBlockRankMode) {
			// calculate block rank
			if (!_firstBlockRankStep) {
				
				if (_blockRankAggregateReady) {
					if (_numBlockRankFinishes == _numSubgraphs) {
						_inBlockRankMode = false;
					}
				}
				
				if (_inBlockRankMode) {
					double sum = 0;
					for (Message message : messages) {
						sum += message.SentPageRank;
					}
					double newBlockPageRank = (1 - D)/_numSubgraphs + D * sum;
					double L1norm = Math.abs(_blockPageRank - newBlockPageRank);
					_blockPageRank = newBlockPageRank;
					
					if (L1norm < BLOCKRANK_EPSILON / _numSubgraphs) {
						// notify everyone that we're ok to finish
						SubGraphMessage s = new SubGraphMessage(Message.finishBlockRankMessage().toBytes());
						for (int partitionId : partitions) {
							sendMessage(partitionId, s);
						}
					}
					
					_blockRankAggregateReady = true;
				}
				
				// make sure we don't accidentally use this later
				messages = Collections.emptyList();
			}
			
			if (_inBlockRankMode) {
				// send outgoing block rank
				Message m = new Message(_blockPageRank / _numBlockOutEdges);
				for (ITemplateVertex remoteVertex : subgraph.remoteVertices()) {
					SubGraphMessage s = new SubGraphMessage(m.toBytes());
					s.setTargetSubgraph(remoteVertex.getRemoteSubgraphId());
					sendMessage(s);
				}
			}
			
			if (_firstBlockRankStep) {
				_firstBlockRankStep = false;
			}
		}
		
		if (!_inBlockRankMode) {
			// calculate approx global page rank
			if (_firstPageRankStep) {
				for (Map.Entry<Long, Double> entry : _localPageRanks.entrySet()) {
					entry.setValue(entry.getValue() * _blockPageRank);
				}
				
				doPageRankIteration(false, messages, true);
			} else {
				if (_pageRankAggregateReady) {
					if (_numPageRankFinishes == _numSubgraphs) {
						voteToHalt();
						
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
				}
					
				if (!isVoteToHalt()) {
					double L1norm = doPageRankIteration(false, messages, false);
					
					if (L1norm < PAGERANK_EPSILON / _numSubgraphs) {
						// notify everyone that we're ok to finish
						SubGraphMessage s = new SubGraphMessage(Message.finishPageRankMessage().toBytes());
						for (int partitionId : partitions) {
							sendMessage(partitionId, s);
						}
					}
					
					_pageRankAggregateReady = true;
				}
			}

			if (_firstPageRankStep) {
				_firstPageRankStep = false;
			}
		}
	}

	private double doPageRankIteration(boolean localOnly, Iterable<Message> messages, boolean sendMessagesOnly) {		
		double L1norm = 0;
		int numVertices = (localOnly ? subgraph.numVertices() : _numVertices);
		
		for (ITemplateVertex vertex : subgraph.vertices()) {
			if (vertex.isRemote()) {
				continue;
			}
			
			_intermediateLocalSums.put(vertex.getId(), 0.0D);
		}
		
		// calculate sums from this subgraphs
		for (ITemplateVertex vertex : subgraph.vertices()) {
			if (vertex.isRemote()) {
				continue;
			}
			
			int outDegree = 0;
			for (ITemplateEdge edge : vertex.outEdges()) {
				if (!localOnly || !edge.getSink(vertex).isRemote()) {
					outDegree++;
				}
			}
			
			double sentWeight = _localPageRanks.get(vertex.getId()) / outDegree;
			
			for (ITemplateEdge edge : vertex.outEdges()) {
				long sinkVertexId = edge.getSink(vertex).getId();
				if (edge.getSink(vertex).isRemote()) {
					if (!localOnly) {
						Message m = new Message(edge.getSink(vertex).getId(), sentWeight);
						SubGraphMessage s = new SubGraphMessage(m.toBytes());
						s.setTargetSubgraph(edge.getSink(vertex).getRemoteSubgraphId());
						sendMessage(s);
					}
				} else {
					_intermediateLocalSums.put(sinkVertexId, _intermediateLocalSums.get(sinkVertexId) + sentWeight);
				}
			}
		}
		
		if (!localOnly) {
			for (Message message : messages) {
				_intermediateLocalSums.put(message.LocalVertexId, _intermediateLocalSums.get(message.LocalVertexId) + message.SentPageRank);
			}
		}
		
		// update weights
		if (!sendMessagesOnly) {
			for (ITemplateVertex vertex : subgraph.vertices()) {
				if (vertex.isRemote()) {
					continue;
				}
				
				double oldPageRank = _localPageRanks.get(vertex.getId());
				double newPageRank = (1 - D)/numVertices + D * _intermediateLocalSums.get(vertex.getId());
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
			} else if (m.FinishBlockRank) {
				_numBlockRankFinishes++;
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
		final boolean FinishBlockRank;
		final boolean FinishPageRank;
		final int NumVertices;
		final long LocalVertexId;
		final double SentPageRank;
		
		private Message(boolean subgraphMessage, boolean finishBlockRank, boolean finishPageRank) {
			SubgraphMessage = subgraphMessage;
			FinishBlockRank = finishBlockRank;
			FinishPageRank = finishPageRank;
			LocalVertexId = Long.MIN_VALUE;
			SentPageRank = 0;
			NumVertices = 0;
		}
		
		static Message subgraphMessage() {
			return new Message(true, false, false);
		}
		
		static Message finishBlockRankMessage() {
			return new Message(false, true, false);
		}
		
		static Message finishPageRankMessage() {
			return new Message(false, false, true);
		}
		
		private Message(int numVertices) {
			LocalVertexId = Long.MIN_VALUE;
			FinishBlockRank = false;
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
			FinishBlockRank = false;
			FinishPageRank = false;
			SentPageRank = sentPageRank;
			NumVertices = 0;
			SubgraphMessage = false;
		}
		
		Message(long localVertexId, double sentPageRank) {
			LocalVertexId = localVertexId;
			FinishBlockRank = false;
			FinishPageRank = false;
			SentPageRank = sentPageRank;
			NumVertices = 0;
			SubgraphMessage = false;
		}
		
		private Message(boolean subgraphMessage, boolean finishBlockRank, boolean finishPageRank, int numVertices, long localVertexId, double sentPageRank) {
			SubgraphMessage = subgraphMessage;
			FinishBlockRank = finishBlockRank;
			FinishPageRank = finishPageRank;
			NumVertices = numVertices;
			LocalVertexId = localVertexId;
			SentPageRank = sentPageRank;
		}
		
		byte[] toBytes() {
			return (Boolean.toString(SubgraphMessage) + "," + Boolean.toString(FinishBlockRank) + "," + Boolean.toString(FinishPageRank) + "," + Integer.toString(NumVertices) + "," + Long.toString(LocalVertexId) + "," + Double.toString(SentPageRank)).getBytes();
		}
		
		static Message fromBytes(byte[] bytes) {
			String[] s = new String(bytes).split(",");
			return new Message(Boolean.parseBoolean(s[0]), Boolean.parseBoolean(s[1]), Boolean.parseBoolean(s[2]), Integer.parseInt(s[3]), Long.parseLong(s[4]), Double.parseDouble(s[5]));
		}
	}
}
