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

import it.unimi.dsi.fastutil.longs.*;

import java.io.*;
import java.util.*;

import edu.usc.goffish.gofs.*;
import edu.usc.goffish.gofs.partition.*;

public final class GMLGraph extends BaseSubgraph {

	protected GMLGraph(long id, ISubgraphTemplate<? extends TemplateVertex, ? extends TemplateEdge> template, PropertySet vertexProperties, PropertySet edgeProperties) {
		super(id, template, vertexProperties, edgeProperties);
	}

	public static GMLGraph read(InputStream gmlTemplateStream) throws IOException {
		return read(1, gmlTemplateStream, true);
	}

	public static GMLGraph read(long id, InputStream gmlTemplateStream) throws IOException {
		return read(id, gmlTemplateStream, true);
	}

	public static GMLGraph read(long id, InputStream gmlTemplateStream, boolean parseProperties) throws IOException {
		Iterable<KeyValuePair> gml = parseProperties ? GMLParser.parse(gmlTemplateStream) : GMLParser.parse(gmlTemplateStream, GMLParser.GRAPH_KEYS);
		return parse(id, gml, parseProperties);
	}

	public static GMLGraph readForceDirectedness(InputStream gmlTemplateStream, boolean isDirectedForce) throws IOException {
		return readForceDirectedness(1, gmlTemplateStream, true, isDirectedForce);
	}

	public static GMLGraph readForceDirectedness(InputStream gmlTemplateStream, boolean parseProperties, boolean isDirectedForce) throws IOException {
		return readForceDirectedness(1, gmlTemplateStream, parseProperties, isDirectedForce);
	}

	public static GMLGraph readForceDirectedness(long id, InputStream gmlTemplateStream, boolean isDirectedForce) throws IOException {
		return readForceDirectedness(id, gmlTemplateStream, true, isDirectedForce);
	}

	public static GMLGraph readForceDirectedness(long id, InputStream gmlTemplateStream, boolean parseProperties, boolean isDirectedForce) throws IOException {
		Iterable<KeyValuePair> gml = parseProperties ? GMLParser.parse(gmlTemplateStream) : GMLParser.parse(gmlTemplateStream, GMLParser.GRAPH_KEYS);
		return parse(id, gml, parseProperties, isDirectedForce);
	}

	private static GMLGraph parse(long id, Iterable<KeyValuePair> gml, boolean parseProperties) {
		Iterable<KeyValuePair> graph_properties_tree = GMLParser.getKVPForKey(gml, GMLParser.GRAPH_KEY).ValueAsList();
		boolean isDirected = (1 == GMLParser.getKVPForKey(graph_properties_tree, GMLParser.GRAPH_DIRECTED_KEY, 0L).ValueAsLong());

		return parse(id, gml, parseProperties, isDirected);
	}

	private static GMLGraph parse(long id, Iterable<KeyValuePair> gml, boolean parseProperties, boolean isDirectedForce) {
		if (gml == null) {
			throw new IllegalArgumentException();
		}

		TemplateGraph graph = new TemplateGraph(isDirectedForce);

		Iterable<KeyValuePair> graph_properties_tree = GMLParser.getKVPForKey(gml, GMLParser.GRAPH_KEY).ValueAsList();

		// if graph has same directedness as force, we don't need to force anything
		boolean forcing = ((1 == GMLParser.getKVPForKey(graph_properties_tree, GMLParser.GRAPH_DIRECTED_KEY, 0L).ValueAsLong()) != isDirectedForce);

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
			KeyValuePair vertex_properties = GMLParser.getKVPForKey(graph_properties_tree, GMLParser.GRAPH_VERTEX_PROPERTIES_KEY);
			if (vertex_properties != null) {
				// iterate over each property
				for (KeyValuePair kvp_property : vertex_properties.ValueAsList()) {
					if (GMLParser.VERTEX_KEYS.contains(kvp_property.Key())) {
						// property names may not conflict with predefined node keys
						throw new GMLFormatException("property name \"" + kvp_property.Key() + "\" may not conflict with predefined node keys");
					}

					boolean is_static = (1L == GMLParser.getKVPForKey(kvp_property.ValueAsList(), GMLParser.GRAPH_PROPERTY_IS_STATIC, 1L).ValueAsLong());
					KeyValuePair kvpType = GMLParser.getKVPForKey(kvp_property.ValueAsList(), GMLParser.GRAPH_PROPERTY_TYPE);
					if (kvpType == null) {
						throw new GMLFormatException("vertex property " + kvp_property.Key() + " must have a type attribute");
					}
					Class<? extends Object> class_type = GMLParser.identifyClassType(kvpType.ValueAsString());

					vertexPropertyIsStaticMap.put(kvp_property.Key(), is_static);
					vertexPropertyTypeMap.put(kvp_property.Key(), class_type);
					vertexPropertyValueMap.put(kvp_property.Key(), new Long2ObjectAVLTreeMap<Object>());
				}
			}

			// edge property information
			edgePropertyIsStaticMap = new TreeMap<>();
			edgePropertyTypeMap = new TreeMap<>();
			edgePropertyValueMap = new HashMap<>();

			// get edge property information
			KeyValuePair edge_properties = GMLParser.getKVPForKey(graph_properties_tree, GMLParser.GRAPH_EDGE_PROPERTIES_KEY);
			if (edge_properties != null) {
				// iterate over each property
				for (KeyValuePair kvp_property : edge_properties.ValueAsList()) {
					if (GMLParser.EDGE_KEYS.contains(kvp_property.Key())) {
						// property names may not conflict with predefined edge keys
						throw new GMLFormatException("property name \"" + kvp_property.Key() + "\" may not conflict with predefined edge keys");
					}

					boolean is_static = (1L == GMLParser.getKVPForKey(kvp_property.ValueAsList(), GMLParser.GRAPH_PROPERTY_IS_STATIC, 1L).ValueAsLong());
					KeyValuePair kvpType = GMLParser.getKVPForKey(kvp_property.ValueAsList(), GMLParser.GRAPH_PROPERTY_TYPE);
					if (kvpType == null) {
						throw new GMLFormatException("edge property " + kvp_property.Key() + " must have a type attribute");
					}
					Class<? extends Object> class_type = GMLParser.identifyClassType(kvpType.ValueAsString());

					edgePropertyIsStaticMap.put(kvp_property.Key(), is_static);
					edgePropertyTypeMap.put(kvp_property.Key(), class_type);
					edgePropertyValueMap.put(kvp_property.Key(), new Long2ObjectAVLTreeMap<Object>());
				}
			}
		}

		// iterate over vertices
		Iterator<KeyValuePair> it_vertices = new KVPIterator(graph_properties_tree, GMLParser.VERTEX_KEY);
		while (it_vertices.hasNext()) {
			Iterable<KeyValuePair> vertex_properties_tree = it_vertices.next().ValueAsList();
			long vertexId = GMLParser.getKVPForKey(vertex_properties_tree, GMLParser.VERTEX_ID_KEY).ValueAsLong();
			KeyValuePair remote = GMLParser.getKVPForKey(vertex_properties_tree, GMLParser.VERTEX_REMOTE_KEY);

			TemplateVertex vertex;
			if (remote != null) {
				long remotePartition = remote.ValueAsLong();
				if (remotePartition > Integer.MAX_VALUE || remotePartition < Integer.MIN_VALUE) {
					// remote partition must fit in integer
					throw new GMLFormatException("vertex with id \"" + vertexId + "\" must have integer remote partition");
				}
				vertex = new TemplateRemoteVertex(vertexId, (int)remotePartition);
			} else {
				vertex = new TemplateVertex(vertexId);
			}

			boolean ok = graph.addVertex(vertex);
			if (!ok) {
				// duplicate node id
				throw new GMLFormatException("duplicate node id \"" + vertexId + "\"");
			}

			// load vertex properties
			if (remote == null && parseProperties) {
				for (KeyValuePair kvp : vertex_properties_tree) {
					if (vertexPropertyValueMap.containsKey(kvp.Key())) {
						vertexPropertyValueMap.get(kvp.Key()).put(vertexId, GMLParser.convertGMLValueToType(kvp.Value(), vertexPropertyTypeMap.get(kvp.Key())));
					}
				}
			}
		}

		// iterate over edges
		Iterator<KeyValuePair> it_edges = new KVPIterator(graph_properties_tree, GMLParser.EDGE_KEY);
		while (it_edges.hasNext()) {
			Iterable<KeyValuePair> edge_properties_tree = it_edges.next().ValueAsList();

			long edgeId = GMLParser.getKVPForKey(edge_properties_tree, GMLParser.EDGE_ID_KEY).ValueAsLong();
			long edgeSource = GMLParser.getKVPForKey(edge_properties_tree, GMLParser.EDGE_SOURCE_KEY).ValueAsLong();
			long edgeSink = GMLParser.getKVPForKey(edge_properties_tree, GMLParser.EDGE_SINK_KEY).ValueAsLong();

			// if forcing undirected, (u, v) and (v, u) directed edge pairs need to be converted into a single
			// undirected edge
			if (!forcing || isDirectedForce || !graph.containsEdge(edgeId, edgeSource, edgeSink)) {
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
				Long2ObjectMap<Object> defaults = new Long2ObjectOpenHashMap<>(vertexPropertyValueMap.get(property), 1f);

				vertexProperties.add(new Property(property, type, is_static, defaults, false));
			}

			// collect edge properties
			edgeProperties = new LinkedList<Property>();
			for (String property : edgePropertyValueMap.keySet()) {
				Class<? extends Object> type = edgePropertyTypeMap.get(property);
				boolean is_static = edgePropertyIsStaticMap.get(property);
				Long2ObjectMap<Object> defaults = new Long2ObjectOpenHashMap<>(edgePropertyValueMap.get(property), 1f);

				edgeProperties.add(new Property(property, type, is_static, defaults, false));
			}
		}

		if (parseProperties) {
			return new GMLGraph(id, graph, new PropertySet(vertexProperties), new PropertySet(edgeProperties));
		} else {
			return new GMLGraph(id, graph, PropertySet.EmptyPropertySet, PropertySet.EmptyPropertySet);
		}
	}

	public static void write(ISubgraph subgraph, OutputStream gmlOutput) throws IOException {
		try (GMLWriter output = new GMLWriter(gmlOutput)) {
			output.writeListOpen(GMLParser.GRAPH_KEY);
			output.write(GMLParser.GRAPH_DIRECTED_KEY, subgraph.isDirected() ? 1L : 0L);

			// write vertex properties
			if (!subgraph.getVertexProperties().isEmpty()) {
				output.writeListOpen(GMLParser.GRAPH_VERTEX_PROPERTIES_KEY);
				for (Property property : subgraph.getVertexProperties()) {
					output.writeListOpen(property.getName());
					output.write(GMLParser.GRAPH_PROPERTY_IS_STATIC, property.isStatic() ? 1L : 0L);
					output.write(GMLParser.GRAPH_PROPERTY_TYPE, GMLWriter.classTypeToGMLType(property.getType()));
					output.writeListClose();
				}
				output.writeListClose();
			}

			// write edge properties
			if (!subgraph.getEdgeProperties().isEmpty()) {
				output.writeListOpen(GMLParser.GRAPH_EDGE_PROPERTIES_KEY);
				for (Property property : subgraph.getEdgeProperties()) {
					output.writeListOpen(property.getName());
					output.write(GMLParser.GRAPH_PROPERTY_IS_STATIC, property.isStatic() ? 1L : 0L);
					output.write(GMLParser.GRAPH_PROPERTY_TYPE, GMLWriter.classTypeToGMLType(property.getType()));
					output.writeListClose();
				}
				output.writeListClose();
			}

			// write vertices
			for (ITemplateVertex vertex : subgraph.getTemplate().vertices()) {
				long vertexId = vertex.getId();

				output.writeListOpen(GMLParser.VERTEX_KEY);
				output.write(GMLParser.VERTEX_ID_KEY, vertexId);
				if (vertex.isRemote()) {
					// write remote vertex
					output.write(GMLParser.VERTEX_REMOTE_KEY, vertex.getRemotePartitionId());
					// TODO: write remote subgraph as well?
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
			for (ITemplateEdge edge : subgraph.getTemplate().edges()) {
				long edgeId = edge.getId();

				output.writeListOpen(GMLParser.EDGE_KEY);
				output.write(GMLParser.EDGE_ID_KEY, edgeId);
				output.write(GMLParser.EDGE_SOURCE_KEY, edge.getSource().getId());
				output.write(GMLParser.EDGE_SINK_KEY, edge.getSink().getId());

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
