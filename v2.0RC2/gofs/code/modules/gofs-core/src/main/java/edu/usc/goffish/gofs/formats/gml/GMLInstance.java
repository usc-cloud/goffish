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

public final class GMLInstance extends BaseInstance {

	protected GMLInstance(long instanceId, long timestampStart, long timestampEnd, ISubgraph subgraph, PropertySet vertexProperties, PropertySet edgeProperties) {
		super(instanceId, timestampStart, timestampEnd, subgraph, vertexProperties, edgeProperties);
	}

	public static GMLInstance read(InputStream gmlInstanceStream, ISubgraph subgraph, PropertySet vertexProperties, PropertySet edgeProperties) throws IOException {
		return parse(GMLParser.parse(gmlInstanceStream), subgraph, vertexProperties, edgeProperties);
	}

	private static GMLInstance parse(Iterable<KeyValuePair> gml, ISubgraph subgraph, PropertySet vertexProperties, PropertySet edgeProperties) throws IOException {
		Iterable<KeyValuePair> graph_properties_tree = GMLParser.getKVPForKey(gml, GMLParser.GRAPH_KEY).ValueAsList();

		long instanceId = GMLParser.getKVPForKey(graph_properties_tree, GMLParser.GRAPH_INSTANCE_ID_KEY).ValueAsLong();
		long timestampStart = GMLParser.getKVPForKey(graph_properties_tree, GMLParser.GRAPH_INSTANCE_TIMESTAMP_START_KEY).ValueAsLong();
		long timestampEnd = GMLParser.getKVPForKey(graph_properties_tree, GMLParser.GRAPH_INSTANCE_TIMESTAMP_END_KEY).ValueAsLong();

		GMLInstance instance = new GMLInstance(instanceId, timestampStart, timestampEnd, subgraph, vertexProperties, edgeProperties);

		// load vertex information
		Iterator<KeyValuePair> it_vertices = new KVPIterator(graph_properties_tree, GMLParser.VERTEX_KEY);
		while (it_vertices.hasNext()) {
			Iterable<KeyValuePair> vertex_properties_tree = it_vertices.next().ValueAsList();
			long vertexId = GMLParser.getKVPForKey(vertex_properties_tree, GMLParser.VERTEX_ID_KEY).ValueAsLong();

			// skip vertex if it doesn't exist in template
			if (!subgraph.containsVertex(vertexId)) {
				continue;
			}

			// load vertex properties
			InstancePropertyMap vertexPropertiesMap = new InstancePropertyMap(vertexId, vertexProperties);
			for (KeyValuePair kvp : vertex_properties_tree) {
				Property property = vertexProperties.getProperty(kvp.Key());
				if (property != null) {
					vertexPropertiesMap.setProperty(kvp.Key(), GMLParser.convertGMLValueToType(kvp.Value(), property.getType()));
				}
			}

			// add to instance if not empty
			if (vertexPropertiesMap.hasSpecifiedProperties()) {
				instance.setPropertiesForVertex(vertexPropertiesMap);
			}
		}

		// load edge information
		Iterator<KeyValuePair> it_edges = new KVPIterator(graph_properties_tree, GMLParser.EDGE_KEY);
		while (it_edges.hasNext()) {
			Iterable<KeyValuePair> edge_properties_tree = it_edges.next().ValueAsList();
			long edgeId = GMLParser.getKVPForKey(edge_properties_tree, GMLParser.EDGE_ID_KEY).ValueAsLong();
			long edgeSource = GMLParser.getKVPForKey(edge_properties_tree, GMLParser.EDGE_SOURCE_KEY).ValueAsLong();

			// skip edge if it doesn't exist in template
			if (!subgraph.containsVertex(edgeSource)) {
				continue;
			}

			// load edge properties
			InstancePropertyMap edgePropertiesMap = new InstancePropertyMap(edgeId, edgeProperties);
			for (KeyValuePair kvp : edge_properties_tree) {
				Property prop = edgeProperties.getProperty(kvp.Key());
				if (prop != null) {
					edgePropertiesMap.setProperty(kvp.Key(), GMLParser.convertGMLValueToType(kvp.Value(), prop.getType()));
				}
			}

			// add to instance if not empty
			if (edgePropertiesMap.hasSpecifiedProperties()) {
				instance.setPropertiesForEdge(edgePropertiesMap);
			}
		}

		return instance;
	}

	public static Long2ObjectMap<GMLInstance> read(InputStream gmlInstance, IPartition partition, PropertySet vertexProperties, PropertySet edgeProperties) throws IOException {
		// read everything in one go for perf reasons, rather than looping over each subgraph

		Iterable<KeyValuePair> gml = GMLParser.parse(gmlInstance);

		Iterable<KeyValuePair> graph_properties_tree = GMLParser.getKVPForKey(gml, GMLParser.GRAPH_KEY).ValueAsList();

		long instanceId = GMLParser.getKVPForKey(graph_properties_tree, GMLParser.GRAPH_INSTANCE_ID_KEY).ValueAsLong();
		long timestampStart = GMLParser.getKVPForKey(graph_properties_tree, GMLParser.GRAPH_INSTANCE_TIMESTAMP_START_KEY).ValueAsLong();
		long timestampEnd = GMLParser.getKVPForKey(graph_properties_tree, GMLParser.GRAPH_INSTANCE_TIMESTAMP_END_KEY).ValueAsLong();

		// define map
		Long2ObjectMap<GMLInstance> instances = new Long2ObjectOpenHashMap<>(partition.size(), 1f);
		for (ISubgraph subgraph : partition) {
			instances.put(subgraph.getId(), new GMLInstance(instanceId, timestampStart, timestampEnd, subgraph, vertexProperties, edgeProperties));
		}

		// load vertex information
		Iterator<KeyValuePair> it_vertices = new KVPIterator(graph_properties_tree, GMLParser.VERTEX_KEY);
		while (it_vertices.hasNext()) {
			Iterable<KeyValuePair> vertex_properties_tree = it_vertices.next().ValueAsList();
			long vertexId = GMLParser.getKVPForKey(vertex_properties_tree, GMLParser.VERTEX_ID_KEY).ValueAsLong();

			// load vertex properties
			InstancePropertyMap vertexPropertiesMap = new InstancePropertyMap(vertexId, vertexProperties);
			for (KeyValuePair kvp : vertex_properties_tree) {
				Property property = vertexProperties.getProperty(kvp.Key());
				if (property != null) {
					vertexPropertiesMap.setProperty(kvp.Key(), GMLParser.convertGMLValueToType(kvp.Value(), property.getType()));
				}
			}

			// add to instance if not empty
			if (vertexPropertiesMap.hasSpecifiedProperties()) {
				// retrieve the subgraph this vertex belongs to
				ISubgraph subgraph = partition.getSubgraphForVertex(vertexId);
				if (subgraph == null) {
					// vertex not found in partition
					throw new IllegalStateException();
				}

				instances.get(subgraph.getId()).setPropertiesForVertex(vertexPropertiesMap);
			}
		}

		// load edge information
		Iterator<KeyValuePair> it_edges = new KVPIterator(graph_properties_tree, GMLParser.EDGE_KEY);
		while (it_edges.hasNext()) {
			Iterable<KeyValuePair> edge_properties_tree = it_edges.next().ValueAsList();
			long edgeId = GMLParser.getKVPForKey(edge_properties_tree, GMLParser.EDGE_ID_KEY).ValueAsLong();
			long edgeSource = GMLParser.getKVPForKey(edge_properties_tree, GMLParser.EDGE_SOURCE_KEY).ValueAsLong();

			// load edge properties
			InstancePropertyMap edgePropertiesMap = new InstancePropertyMap(edgeId, edgeProperties);
			for (KeyValuePair kvp : edge_properties_tree) {
				Property prop = edgeProperties.getProperty(kvp.Key());
				if (prop != null) {
					edgePropertiesMap.setProperty(kvp.Key(), GMLParser.convertGMLValueToType(kvp.Value(), prop.getType()));
				}
			}

			// add to instance if not empty
			if (edgePropertiesMap.hasSpecifiedProperties()) {
				// retrieve the subgraph this vertex belongs to
				ISubgraph subgraph = partition.getSubgraphForVertex(edgeSource);
				if (subgraph == null) {
					// vertex not found in partition
					throw new IllegalStateException();
				}

				instances.get(subgraph.getId()).setPropertiesForEdge(edgePropertiesMap);
			}
		}

		return instances;
	}

	public static void write(ISubgraphInstance instance, OutputStream gmlOutput) throws IOException {
		try (GMLWriter output = new GMLWriter(gmlOutput)) {
			output.writeListOpen(GMLParser.GRAPH_KEY);
			output.write(GMLParser.GRAPH_INSTANCE_ID_KEY, instance.getId());
			output.write(GMLParser.GRAPH_INSTANCE_TIMESTAMP_START_KEY, instance.getTimestampStart());
			output.write(GMLParser.GRAPH_INSTANCE_TIMESTAMP_END_KEY, instance.getTimestampEnd());

			// write vertex values
			for (ISubgraphObjectProperties vertexProperties : instance.getPropertiesForVertices()) {
				Collection<String> properties = vertexProperties.getSpecifiedProperties();
				if (properties.isEmpty()) {
					continue;
				}

				output.writeListOpen(GMLParser.VERTEX_KEY);
				output.write(GMLParser.VERTEX_ID_KEY, vertexProperties.getId());
				for (String property : properties) {
					output.write(KeyValuePair.createKVP(property, GMLWriter.classValueToGMLValue(vertexProperties.getValue(property))));
				}
				output.writeListClose();
			}

			// write edge values - we iterate over edges rather than edges with
			// properties so we can get source/sink information easily
			for (ITemplateEdge edge : instance.getTemplate().edges()) {
				ISubgraphObjectProperties edgeProperties = instance.getPropertiesForEdge(edge.getId());
				if (edgeProperties == null) {
					continue;
				}
				Collection<String> properties = edgeProperties.getSpecifiedProperties();
				if (properties.isEmpty()) {
					continue;
				}

				output.writeListOpen(GMLParser.EDGE_KEY);
				output.write(GMLParser.EDGE_ID_KEY, edge.getId());
				output.write(GMLParser.EDGE_SOURCE_KEY, edge.getSource().getId());
				output.write(GMLParser.EDGE_SINK_KEY, edge.getSink().getId());
				for (String property : properties) {
					output.write(KeyValuePair.createKVP(property, GMLWriter.classValueToGMLValue(edgeProperties.getValue(property))));
				}
				output.writeListClose();
			}

			output.writeListClose();
		}
	}
}
