/*
*  Licensed to the Apache Software Foundation (ASF) under one
*  or more contributor license agreements.  See the NOTICE file
*  distributed with this work for additional information
*  regarding copyright ownership.  The ASF licenses this file
*  to you under the Apache License, Version 2.0 (the
*  "License"); you may not use this file except in compliance
*  with the License.  You may obtain a copy of the License at
*
*   http://www.apache.org/licenses/LICENSE-2.0
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

	public void partitionTemplate(InputStream gmlTemplateStream, Path outputDirectory, String prefix, String postfix) throws IOException {
		partitionTemplate(GMLParser.parse(gmlTemplateStream), outputDirectory, prefix, postfix);
	}

	public void partitionTemplate(File gmlTemplateFile, Path outputDirectory, String prefix, String postfix) throws IOException {
		partitionTemplate(GMLParser.parse(gmlTemplateFile), outputDirectory, prefix, postfix);
	}
	
	private void partitionTemplate(Iterable<KeyValuePair> gmlTemplate, Path outputDirectory, String prefix, String postfix) throws IOException {
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

		for (int partitionId : _partitioning.getPartitions()) {
			Path outputPath = outputDirectory.resolve(prefix + partitionId + postfix);
			try (GMLWriter output = new GMLWriter(Files.newOutputStream(outputPath))) {

				// iterate at top level
				for (KeyValuePair kvp : gmlTemplate) {
					if (kvp.Key().equals(GMLParser.GraphKey)) {
						output.writeListOpen(GMLParser.GraphKey);

						// vertex id to partition mapping
						Long2IntMap remoteVertices = new Long2IntRBTreeMap();

						boolean isDirected = GMLParser.getKVPForKey(kvp.ValueAsList(), GMLParser.GraphDirectedKey, 0L).ValueAsLong() == 1L;
						_directed = isDirected;

						// iterate at graph level
						for (KeyValuePair graphElementKvp : kvp.ValueAsList()) {
							if (graphElementKvp.Key().equals(GMLParser.VertexKey)) {
								long vertexId = GMLParser.getKVPForKey(graphElementKvp.ValueAsList(), GMLParser.VertexIdKey).ValueAsLong();
								// write vertex if in the current partition
								if (_partitioning.get(vertexId) == partitionId) {
									output.write(graphElementKvp);
								}
							} else if (graphElementKvp.Key().equals(GMLParser.EdgeKey)) {
								long edgeSourceId = GMLParser.getKVPForKey(graphElementKvp.ValueAsList(), GMLParser.EdgeSourceKey).ValueAsLong();
								long edgeSinkId = GMLParser.getKVPForKey(graphElementKvp.ValueAsList(), GMLParser.EdgeSinkKey).ValueAsLong();

								int edgeSinkPartitionId = _partitioning.get(edgeSinkId);
								int edgeSourcePartitionId = _partitioning.get(edgeSourceId);
								
								// write edge if source in current partition, or
								// if undirected and the sink is in current
								// partition
								if (edgeSourcePartitionId == partitionId || (!isDirected && edgeSinkPartitionId == partitionId)) {
									output.write(graphElementKvp);

									if (edgeSinkPartitionId != partitionId) {
										// if sink is not in current partition we
										// need a remote vertex
										remoteVertices.put(edgeSinkId, edgeSinkPartitionId);
									} else if (!isDirected && edgeSourcePartitionId != partitionId) {
										// if undirected and source is not in
										// current partition we need a remote
										// vertex
										remoteVertices.put(edgeSourceId, edgeSourcePartitionId);
									}
								}
							} else {
								output.write(graphElementKvp);
							}
						}

						// write remote vertices
						for (Long2IntMap.Entry entry : remoteVertices.long2IntEntrySet()) {
							output.writeListOpen(GMLParser.VertexKey);
							output.write(new LongKeyValuePair(GMLParser.VertexIdKey, entry.getLongKey()));
							output.write(new LongKeyValuePair(GMLParser.VertexRemoteKey, entry.getIntValue()));
							output.writeListClose();
						}

						output.writeListClose();
					} else {
						output.write(kvp);
					}
				}
			}
		}

		_templateParsed = true;
	}

	public void partitionInstance(InputStream gmlInstanceFile, Path outputDirectory, String prefix, String postfix) throws IOException {
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
		for (int partitionId : _partitioning.getPartitions()) {
			Path outputPath = outputDirectory.resolve(prefix + partitionId + postfix);
			try (GMLWriter output = new GMLWriter(Files.newOutputStream(outputPath))) {

				// iterate at top level
				for (KeyValuePair kvp : gml) {
					if (kvp.Key().equals(GMLParser.GraphKey)) {
						output.writeListOpen(GMLParser.GraphKey);

						// iterate at graph level
						for (KeyValuePair graphElementKvp : kvp.ValueAsList()) {
							if (graphElementKvp.Key().equals(GMLParser.VertexKey)) {
								long vertexId = GMLParser.getKVPForKey(graphElementKvp.ValueAsList(), GMLParser.VertexIdKey).ValueAsLong();
								// write vertex if in the current partition
								if (_partitioning.get(vertexId) == partitionId) {
									output.write(graphElementKvp);
								}
							} else if (graphElementKvp.Key().equals(GMLParser.EdgeKey)) {
								long edgeSourceId = GMLParser.getKVPForKey(graphElementKvp.ValueAsList(), GMLParser.EdgeSourceKey).ValueAsLong();
								long edgeSinkId = GMLParser.getKVPForKey(graphElementKvp.ValueAsList(), GMLParser.EdgeSinkKey).ValueAsLong();

								// write edge if source in current partition, or
								// if undirected and the sink is in current
								// partition
								if (_partitioning.get(edgeSourceId) == partitionId || (!_directed && _partitioning.get(edgeSinkId) == partitionId)) {
									output.write(graphElementKvp);
								}
							} else {
								output.write(graphElementKvp);
							}
						}

						output.writeListClose();
					} else {
						output.write(kvp);
					}
				}
			}
		}
	}
}
