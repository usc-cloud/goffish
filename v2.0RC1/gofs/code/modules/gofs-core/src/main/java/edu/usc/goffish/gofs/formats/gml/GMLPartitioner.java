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

package edu.usc.goffish.gofs.formats.gml;

import it.unimi.dsi.fastutil.ints.*;
import it.unimi.dsi.fastutil.longs.*;

import java.io.*;
import java.nio.file.*;
import java.util.*;

import edu.usc.goffish.gofs.util.partitioning.*;

public class GMLPartitioner {

	private final IPartitioning _partitioning;

	private boolean _directed;
	private boolean _templateParsed;

	public GMLPartitioner(IPartitioning partitioning) {
		if (partitioning == null) {
			throw new IllegalArgumentException();
		}

		_partitioning = partitioning;

		_directed = false;
		_templateParsed = false;
	}

	public IntSet getPartitions() {
		return _partitioning.getPartitions();
	}

	public long partitionTemplate(InputStream gmlTemplateStream, Path outputDirectory, String prefix, String postfix) throws IOException {
		return partitionTemplate(GMLParser.parse(gmlTemplateStream), outputDirectory, prefix, postfix);
	}

	private long partitionTemplate(Iterable<KeyValuePair> gmlTemplate, Path outputDirectory, String prefix, String postfix) throws IOException {
		if (_templateParsed) {
			// partitionTemplate should only be called once
			throw new IllegalStateException();
		}
		if (gmlTemplate == null) {
			throw new IllegalArgumentException();
		}
		if (outputDirectory == null) {
			throw new IllegalArgumentException();
		}
		if (prefix == null) {
			throw new IllegalArgumentException();
		}
		if (postfix == null) {
			throw new IllegalArgumentException();
		}

		long time = System.currentTimeMillis();

		Int2ObjectMap<GMLWriter> writers = new Int2ObjectArrayMap<GMLWriter>(_partitioning.getPartitions().size());
		try {
			// open all output streams
			for (int partitionId : _partitioning.getPartitions()) {
				Path outputPath = outputDirectory.resolve(prefix + partitionId + postfix);
				writers.put(partitionId, new GMLWriter(Files.newOutputStream(outputPath)));
			}
		
			// prepare remote vertices
			Int2ObjectMap<Long2IntMap> remoteVerticesMap = new Int2ObjectArrayMap<Long2IntMap>(_partitioning.getPartitions().size());
			for (int partitionId : _partitioning.getPartitions()) {
				remoteVerticesMap.put(partitionId, new Long2IntRBTreeMap());
			}
			
			// iterate at top level
			for (KeyValuePair kvp : gmlTemplate) {
				if (kvp.Key().equals(GMLParser.GRAPH_KEY)) {
					writeOpen(writers.values(), GMLParser.GRAPH_KEY);

					_directed = GMLParser.getKVPForKey(kvp.ValueAsList(), GMLParser.GRAPH_DIRECTED_KEY, 0L).ValueAsLong() == 1L;

					// iterate at graph level
					for (KeyValuePair graphElementKvp : kvp.ValueAsList()) {
						if (graphElementKvp.Key().equals(GMLParser.VERTEX_KEY)) {
							long vertexId = GMLParser.getKVPForKey(graphElementKvp.ValueAsList(), GMLParser.VERTEX_ID_KEY).ValueAsLong();
							
							// write vertex to partition
							writers.get(_partitioning.get(vertexId)).write(graphElementKvp);
						} else if (graphElementKvp.Key().equals(GMLParser.EDGE_KEY)) {
							long edgeSourceId = GMLParser.getKVPForKey(graphElementKvp.ValueAsList(), GMLParser.EDGE_SOURCE_KEY).ValueAsLong();
							long edgeSinkId = GMLParser.getKVPForKey(graphElementKvp.ValueAsList(), GMLParser.EDGE_SINK_KEY).ValueAsLong();

							int edgeSourcePartitionId = _partitioning.get(edgeSourceId);
							int edgeSinkPartitionId = _partitioning.get(edgeSinkId);

							// write to source partition
							writers.get(edgeSourcePartitionId).write(graphElementKvp);
							
							// if edge crosses partitions...
							if (edgeSourcePartitionId != edgeSinkPartitionId){
								
								// add remote vertex in source partition
								remoteVerticesMap.get(edgeSourcePartitionId).put(edgeSinkId, edgeSinkPartitionId);
								
								// if undirected edge...
								if (!_directed) {
									
									// write to sink partition
									writers.get(edgeSinkPartitionId).write(graphElementKvp);
									
									// add remote vertex in sink partition
									remoteVerticesMap.get(edgeSinkPartitionId).put(edgeSourceId, edgeSourcePartitionId);
								}
							}
						} else {
							writeAll(writers.values(), graphElementKvp);
						}
					}

					// write remote vertices
					for (int partitionId : remoteVerticesMap.keySet()) {
						GMLWriter output = writers.get(partitionId);
						for (Long2IntMap.Entry entry : remoteVerticesMap.get(partitionId).long2IntEntrySet()) {
							output.writeListOpen(GMLParser.VERTEX_KEY);
							output.write(new LongKeyValuePair(GMLParser.VERTEX_ID_KEY, entry.getLongKey()));
							output.write(new LongKeyValuePair(GMLParser.VERTEX_REMOTE_KEY, entry.getIntValue()));
							output.writeListClose();
						}
					}

					writeClose(writers.values());
				} else {
					writeAll(writers.values(), kvp);
				}
			}
			
		} finally {
			for (GMLWriter writer : writers.values()) {
				writer.close();
			}
		}

		_templateParsed = true;

		return System.currentTimeMillis() - time;
	}
	
	public long partitionInstance(InputStream gmlInstanceFile, Path outputDirectory, String prefix, String postfix) throws IOException {
		if (!_templateParsed) {
			// partitionTemplate must be called before partitionInstance
			throw new IllegalStateException();
		}
		if (gmlInstanceFile == null) {
			throw new IllegalArgumentException();
		}
		if (outputDirectory == null) {
			throw new IllegalArgumentException();
		}
		if (prefix == null) {
			throw new IllegalArgumentException();
		}
		if (postfix == null) {
			throw new IllegalArgumentException();
		}

		Iterable<KeyValuePair> gml = GMLParser.parse(gmlInstanceFile);

		long time = System.currentTimeMillis();
		
		Int2ObjectMap<GMLWriter> writers = new Int2ObjectArrayMap<GMLWriter>(_partitioning.getPartitions().size());
		try {
			// open all output streams
			for (int partitionId : _partitioning.getPartitions()) {
				Path outputPath = outputDirectory.resolve(prefix + partitionId + postfix);
				writers.put(partitionId, new GMLWriter(Files.newOutputStream(outputPath)));
			}
	
			// iterate at top level
			for (KeyValuePair kvp : gml) {
				if (kvp.Key().equals(GMLParser.GRAPH_KEY)) {
					writeOpen(writers.values(), GMLParser.GRAPH_KEY);

					// iterate at graph level
					for (KeyValuePair graphElementKvp : kvp.ValueAsList()) {
						if (graphElementKvp.Key().equals(GMLParser.VERTEX_KEY)) {
							long vertexId = GMLParser.getKVPForKey(graphElementKvp.ValueAsList(), GMLParser.VERTEX_ID_KEY).ValueAsLong();
							
							// write vertex to partition
							writers.get(_partitioning.get(vertexId)).write(graphElementKvp);
						} else if (graphElementKvp.Key().equals(GMLParser.EDGE_KEY)) {
							long edgeSourceId = GMLParser.getKVPForKey(graphElementKvp.ValueAsList(), GMLParser.EDGE_SOURCE_KEY).ValueAsLong();
							long edgeSinkId = GMLParser.getKVPForKey(graphElementKvp.ValueAsList(), GMLParser.EDGE_SINK_KEY).ValueAsLong();

							int edgeSourcePartitionId = _partitioning.get(edgeSourceId);
							int edgeSinkPartitionId = _partitioning.get(edgeSinkId);

							// write to source partition
							writers.get(edgeSourcePartitionId).write(graphElementKvp);
							
							// if edge crosses partitions...
							if (edgeSourcePartitionId != edgeSinkPartitionId && !_directed){
								// write to sink partition
								writers.get(edgeSinkPartitionId).write(graphElementKvp);
							}
						} else {
							writeAll(writers.values(), graphElementKvp);
						}
					}

					writeClose(writers.values());
				} else {
					writeAll(writers.values(), kvp);
				}
			}
		} finally {
			for (GMLWriter writer : writers.values()) {
				writer.close();
			}
		}

		return System.currentTimeMillis() - time;
	}

	private static void writeOpen(Collection<GMLWriter> writers, String key) throws IOException {
		for (GMLWriter writer : writers) {
			writer.writeListOpen(key);
		}
	}

	private static void writeClose(Collection<GMLWriter> writers) throws IOException {
		for (GMLWriter writer : writers) {
			writer.writeListClose();
		}
	}
	
	private static void writeAll(Collection<GMLWriter> writers, KeyValuePair kvp) throws IOException {
		for (GMLWriter writer : writers) {
			writer.write(kvp);
		}
	}
}
