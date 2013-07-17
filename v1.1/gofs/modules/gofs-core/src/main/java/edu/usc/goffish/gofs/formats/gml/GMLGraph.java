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

import it.unimi.dsi.fastutil.longs.*;

import java.io.*;
import java.util.*;

import edu.usc.goffish.gofs.*;
import edu.usc.goffish.gofs.partition.*;

public final class GMLGraph extends BaseSubgraph {

	public GMLGraph(long id, ISubgraphTemplate template, PropertySet vertexProperties, PropertySet edgeProperties, Map<Long, Integer> remoteVertices) {
		super(id, template, vertexProperties, edgeProperties, remoteVertices);
	}


	
	public static GMLGraph read(InputStream gmlTemplateStream) throws IOException {
		return read(0, gmlTemplateStream, true);
	}
	
	public static GMLGraph read(File gmlTemplateFile) throws IOException {
		return read(0, gmlTemplateFile, true);
	}

	public static GMLGraph read(long id, InputStream gmlTemplateStream) throws IOException {
		return read(id, gmlTemplateStream, true);
	}
	
	public static GMLGraph read(long id, File gmlTemplateFile) throws IOException {
		return read(id, gmlTemplateFile, true);
	}	

	public static GMLGraph read(long id, InputStream gmlTemplateStream, boolean parseProperties) throws IOException {
		Iterable<KeyValuePair> gml = parseProperties ? GMLParser.parse(gmlTemplateStream) : GMLParser.parse(gmlTemplateStream, GMLParser.GraphKeys);
		return parse(id, gml, parseProperties);
	}
	
	public static GMLGraph read(long id, File gmlTemplateFile, boolean parseProperties) throws IOException {
		Iterable<KeyValuePair> gml = parseProperties ? GMLParser.parse(gmlTemplateFile) : GMLParser.parse(gmlTemplateFile, GMLParser.GraphKeys);
		return parse(id, gml, parseProperties);
	}

	
	
	public static GMLGraph readForceDirectedness(InputStream gmlTemplateStream, boolean isDirectedForce) throws IOException {
		return readForceDirectedness(0, gmlTemplateStream, true, isDirectedForce);
	}
	
	public static GMLGraph readForceDirectedness(File gmlTemplateFile, boolean isDirectedForce) throws IOException {
		return readForceDirectedness(0, gmlTemplateFile, true, isDirectedForce);
	}
	
	public static GMLGraph readForceDirectedness(InputStream gmlTemplateStream, boolean parseProperties, boolean isDirectedForce) throws IOException {
		return readForceDirectedness(0, gmlTemplateStream, parseProperties, isDirectedForce);
	}
	
	public static GMLGraph readForceDirectedness(File gmlTemplateFile, boolean parseProperties, boolean isDirectedForce) throws IOException {
		return readForceDirectedness(0, gmlTemplateFile, parseProperties, isDirectedForce);
	}
	
	public static GMLGraph readForceDirectedness(long id, InputStream gmlTemplateStream, boolean isDirectedForce) throws IOException {
		return readForceDirectedness(id, gmlTemplateStream, true, isDirectedForce);
	}
	
	public static GMLGraph readForceDirectedness(long id, File gmlTemplateFile, boolean isDirectedForce) throws IOException {
		return readForceDirectedness(id, gmlTemplateFile, true, isDirectedForce);
	}

	public static GMLGraph readForceDirectedness(long id, InputStream gmlTemplateStream, boolean parseProperties, boolean isDirectedForce) throws IOException {
		Iterable<KeyValuePair> gml = parseProperties ? GMLParser.parse(gmlTemplateStream) : GMLParser.parse(gmlTemplateStream, GMLParser.GraphKeys);
		return parse(id, gml, parseProperties, isDirectedForce);
	}
	
	public static GMLGraph readForceDirectedness(long id, File gmlTemplateFile, boolean parseProperties, boolean isDirectedForce) throws IOException {
		Iterable<KeyValuePair> gml = parseProperties ? GMLParser.parse(gmlTemplateFile) : GMLParser.parse(gmlTemplateFile, GMLParser.GraphKeys);
		return parse(id, gml, parseProperties, isDirectedForce);
	}
	
	

	private static GMLGraph parse(long id, Iterable<KeyValuePair> gml, boolean parseProperties) {
		Iterable<KeyValuePair> graph_properties_tree = GMLParser.getKVPForKey(gml, GMLParser.GraphKey).ValueAsList();
		boolean isDirected = (1 == GMLParser.getKVPForKey(graph_properties_tree, GMLParser.GraphDirectedKey, 0L).ValueAsLong());

		return parse(id, gml, parseProperties, isDirected);
	}

	private static GMLGraph parse(long id, Iterable<KeyValuePair> gml, boolean parseProperties, boolean isDirectedForce) {
		if (gml == null) {
			throw new IllegalArgumentException();
		}

		TemplateGraph graph = new TemplateGraph(isDirectedForce);

		Iterable<KeyValuePair> graph_properties_tree = GMLParser.getKVPForKey(gml, GMLParser.GraphKey).ValueAsList();

		TreeMap<String, Boolean> vertexPropertyIsStaticMap = null;
		TreeMap<String, Class<? extends Object>> vertexPropertyTypeMap = null;
		HashMap<String, Long2ObjectMap<Object>> vertexPropertyValueMap = null;

		TreeMap<String, Boolean> edgePropertyIsStaticMap = null;
		TreeMap<String, Class<? extends Object>> edgePropertyTypeMap = null;
		HashMap<String, Long2ObjectMap<Object>> edgePropertyValueMap = null;

		// vertex property information
		if (parseProperties) {
			vertexPropertyIsStaticMap = new TreeMap<>();
			vertexPropertyTypeMap = new TreeMap<>();
			vertexPropertyValueMap = new HashMap<>();

			// get vertex property information
			KeyValuePair vertex_properties = GMLParser.getKVPForKey(graph_properties_tree, GMLParser.GraphVertexPropertiesKey);
			if (vertex_properties != null) {
				// iterate over each property
				for (KeyValuePair kvp_property : vertex_properties.ValueAsList()) {
					if (GMLParser.VertexKeys.contains(kvp_property.Key())) {
						// property names may not conflict with predefined node keys
						throw new GMLFormatException("property name \"" + kvp_property.Key() + "\" may not conflict with predefined node keys");
					}
					
					boolean is_static = (1L == GMLParser.getKVPForKey(kvp_property.ValueAsList(), GMLParser.GraphPropertyIsStatic, 1L).ValueAsLong());
					String type = GMLParser.getKVPForKey(kvp_property.ValueAsList(), GMLParser.GraphPropertyType, GMLParser.StringType).ValueAsString();
					Class<? extends Object> class_type = GMLParser.identifyClassType(type);

					vertexPropertyIsStaticMap.put(kvp_property.Key(), is_static);
					vertexPropertyTypeMap.put(kvp_property.Key(), class_type);
					vertexPropertyValueMap.put(kvp_property.Key(), new Long2ObjectRBTreeMap<Object>());
				}
			}

			// edge property information
			edgePropertyIsStaticMap = new TreeMap<>();
			edgePropertyTypeMap = new TreeMap<>();
			edgePropertyValueMap = new HashMap<>();

			// get edge property information
			KeyValuePair edge_properties = GMLParser.getKVPForKey(graph_properties_tree, GMLParser.GraphEdgePropertiesKey);
			if (edge_properties != null) {
				// iterate over each property
				for (KeyValuePair kvp_property : edge_properties.ValueAsList()) {
					if (GMLParser.EdgeKeys.contains(kvp_property.Key())) {
						// property names may not conflict with predefined edge keys
						throw new GMLFormatException("property name \"" + kvp_property.Key() + "\" may not conflict with predefined edge keys");
					}
					
					boolean is_static = (1L == GMLParser.getKVPForKey(kvp_property.ValueAsList(), GMLParser.GraphPropertyIsStatic, 1L).ValueAsLong());
					String type = GMLParser.getKVPForKey(kvp_property.ValueAsList(), GMLParser.GraphPropertyType, GMLParser.StringType).ValueAsString();
					Class<? extends Object> class_type = GMLParser.identifyClassType(type);

					edgePropertyIsStaticMap.put(kvp_property.Key(), is_static);
					edgePropertyTypeMap.put(kvp_property.Key(), class_type);
					edgePropertyValueMap.put(kvp_property.Key(), new Long2ObjectRBTreeMap<Object>());
				}
			}
		}

		Long2IntMap remoteVertices = new Long2IntOpenHashMap();

		// iterate over vertices
		Iterator<KeyValuePair> it_vertices = new KVPIterator(graph_properties_tree, GMLParser.VertexKey);
		while (it_vertices.hasNext()) {
			Iterable<KeyValuePair> vertex_properties_tree = it_vertices.next().ValueAsList();
			long vertexId = GMLParser.getKVPForKey(vertex_properties_tree, GMLParser.VertexIdKey).ValueAsLong();
			KeyValuePair remote = GMLParser.getKVPForKey(vertex_properties_tree, GMLParser.VertexRemoteKey);
			
			boolean ok = graph.addVertex(new TemplateVertex(vertexId, remote != null));
			if (!ok) {
				// duplicate node id
				throw new GMLFormatException("duplicate node id \"" + vertexId + "\"");
			}

			// save remote vertices for later
			if (remote != null) {
				long remotePartition = remote.ValueAsLong();
				if (remotePartition > Integer.MAX_VALUE || remotePartition < Integer.MIN_VALUE) {
					// remote partition must fit in integer
					throw new GMLFormatException("vertex with id \"" + vertexId + "\" must have integer remote partition");
				}
				remoteVertices.put(vertexId, (int)remotePartition);
				// ignore properties for remote vertices
			} else if (parseProperties) {
				// load vertex properties
				for (KeyValuePair kvp : vertex_properties_tree) {
					if (vertexPropertyValueMap.containsKey(kvp.Key())) {
						vertexPropertyValueMap.get(kvp.Key()).put(vertexId, GMLParser.convertGMLValueToType(kvp.Value(), vertexPropertyTypeMap.get(kvp.Key())));
					}
				}
			}
		}

		// iterate over edges
		Iterator<KeyValuePair> it_edges = new KVPIterator(graph_properties_tree, GMLParser.EdgeKey);
		while (it_edges.hasNext()) {
			Iterable<KeyValuePair> edge_properties_tree = it_edges.next().ValueAsList();

			long edgeId = GMLParser.getKVPForKey(edge_properties_tree, GMLParser.EdgeIdKey).ValueAsLong();
			long edgeSource = GMLParser.getKVPForKey(edge_properties_tree, GMLParser.EdgeSourceKey).ValueAsLong();
			long edgeSink = GMLParser.getKVPForKey(edge_properties_tree, GMLParser.EdgeSinkKey).ValueAsLong();

			// TODO: get rid of this undirected force if we can
			
			// if forcing undirected, (u, v) and (v, u) directed edge pairs
			// should be converted into a single undirected edge
			if (isDirectedForce || !graph.containsEdge(edgeId, edgeSource, edgeSink)) {
				boolean ok = graph.connectEdge(new TemplateEdge(edgeId, graph.getVertex(edgeSource), graph.getVertex(edgeSink)));
				if (!ok) {
					// edge is invalid or already exists
					throw new GMLFormatException("edge with id \"" + edgeId + "\" is invalid or already exists");
				}
			}

			if (parseProperties) {
				// load edge properties
				for (KeyValuePair kvp : edge_properties_tree) {
					if (edgePropertyValueMap.containsKey(kvp.Key())) {
						edgePropertyValueMap.get(kvp.Key()).put(edgeId, GMLParser.convertGMLValueToType(kvp.Value(), edgePropertyTypeMap.get(kvp.Key())));
					}
				}
			}
		}

		LinkedList<Property> vertexProperties = null;
		LinkedList<Property> edgeProperties = null;

		if (parseProperties) {
			// collect vertex properties
			vertexProperties = new LinkedList<Property>();
			for (String property : vertexPropertyValueMap.keySet()) {
				Class<? extends Object> type = vertexPropertyTypeMap.get(property);
				boolean is_static = vertexPropertyIsStaticMap.get(property);
				Long2ObjectMap<Object> defaults = vertexPropertyValueMap.get(property);

				vertexProperties.add(new Property(property, type, is_static, defaults));
			}

			// collect edge properties
			edgeProperties = new LinkedList<Property>();
			for (String property : edgePropertyValueMap.keySet()) {
				Class<? extends Object> type = edgePropertyTypeMap.get(property);
				boolean is_static = edgePropertyIsStaticMap.get(property);
				Long2ObjectMap<Object> defaults = edgePropertyValueMap.get(property);

				edgeProperties.add(new Property(property, type, is_static, defaults));
			}
		}

		if (parseProperties) {
			return new GMLGraph(id, graph, new PropertySet(vertexProperties), new PropertySet(edgeProperties), remoteVertices);
		} else {
			return new GMLGraph(id, graph, PropertySet.EmptyPropertySet, PropertySet.EmptyPropertySet, remoteVertices);
		}
	}

	public static void write(ISubgraph subgraph, OutputStream gmlOutput) throws IOException {
		try (GMLWriter output = new GMLWriter(gmlOutput)) {
			output.writeListOpen(GMLParser.GraphKey);
			output.write(GMLParser.GraphDirectedKey, subgraph.isDirected() ? 1L : 0L);

			// write vertex properties
			if (!subgraph.getVertexProperties().isEmpty()) {
				output.writeListOpen(GMLParser.GraphVertexPropertiesKey);
				for (Property property : subgraph.getVertexProperties()) {
					output.writeListOpen(property.getName());
					output.write(GMLParser.GraphPropertyIsStatic, property.isStatic() ? 1L : 0L);
					output.write(GMLParser.GraphPropertyType, GMLWriter.classTypeToGMLType(property.getType()));
					output.writeListClose();
				}
				output.writeListClose();
			}

			// write edge properties
			if (!subgraph.getEdgeProperties().isEmpty()) {
				output.writeListOpen(GMLParser.GraphEdgePropertiesKey);
				for (Property property : subgraph.getEdgeProperties()) {
					output.writeListOpen(property.getName());
					output.write(GMLParser.GraphPropertyIsStatic, property.isStatic() ? 1L : 0L);
					output.write(GMLParser.GraphPropertyType, GMLWriter.classTypeToGMLType(property.getType()));
					output.writeListClose();
				}
				output.writeListClose();
			}

			// write vertices
			for (TemplateVertex vertex : subgraph.getTemplate().vertices()) {
				long vertexId = vertex.getId();

				output.writeListOpen(GMLParser.VertexKey);
				output.write(GMLParser.VertexIdKey, vertexId);
				if (subgraph.isRemoteVertex(vertexId)) {
					// write remote vertex
					output.write(GMLParser.VertexRemoteKey, subgraph.getPartitionForRemoteVertex(vertexId));
				} else {
					// write vertex properties
					for (Property property : subgraph.getVertexProperties()) {
						if (property.getDefaults().containsKey(vertexId)) {
							output.write(KeyValuePair.createKVP(property.getName(), property.getDefaults().get(vertexId)));
						}
					}
				}
				output.writeListClose();
			}

			// write edges
			for (TemplateEdge edge : subgraph.getTemplate().edges()) {
				long edgeId = edge.getId();

				output.writeListOpen(GMLParser.EdgeKey);
				output.write(GMLParser.EdgeIdKey, edgeId);
				output.write(GMLParser.EdgeSourceKey, edge.getSource().getId());
				output.write(GMLParser.EdgeSinkKey, edge.getSink().getId());

				// write edge properties
				for (Property property : subgraph.getEdgeProperties()) {
					if (property.getDefaults().containsKey(edgeId)) {
						output.write(KeyValuePair.createKVP(property.getName(), property.getDefaults().get(edgeId)));
					}
				}

				output.writeListClose();
			}

			output.writeListClose();
		}
	}
}
