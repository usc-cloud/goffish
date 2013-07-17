/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law
 * or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */

package edu.usc.goffish.gofs.formats.gml;

import java.io.*;
import java.util.*;

import edu.usc.goffish.gofs.*;
import edu.usc.goffish.gofs.partition.*;

public final class GMLInstance extends Instance {

	public GMLInstance(long instanceId, long timestampStart, long timestampEnd, ISubgraphTemplate template, PropertySet vertexProperties, PropertySet edgeProperties) {
		super(instanceId, timestampStart, timestampEnd, template, vertexProperties, edgeProperties);
	}

	public static GMLInstance read(InputStream gmlInstanceStream, ISubgraphTemplate template, PropertySet vertexProperties, PropertySet edgeProperties) throws IOException {
		return parse(GMLParser.parse(gmlInstanceStream), template, vertexProperties, edgeProperties);
	}
	
	public static GMLInstance read(File gmlInstanceFile, ISubgraphTemplate template, PropertySet vertexProperties, PropertySet edgeProperties) throws IOException {
		return parse(GMLParser.parse(gmlInstanceFile), template, vertexProperties, edgeProperties);
	}

	private static GMLInstance parse(Iterable<KeyValuePair> gml, ISubgraphTemplate template, PropertySet vertexProperties, PropertySet edgeProperties) throws IOException {
		Iterable<KeyValuePair> graph_properties_tree = GMLParser.getKVPForKey(gml, GMLParser.GraphKey).ValueAsList();

		long instanceId = GMLParser.getKVPForKey(graph_properties_tree, GMLParser.GraphInstanceIdKey).ValueAsLong();
		long timestampStart = GMLParser.getKVPForKey(graph_properties_tree, GMLParser.GraphInstanceTimestampStartKey).ValueAsLong();
		long timestampEnd = GMLParser.getKVPForKey(graph_properties_tree, GMLParser.GraphInstanceTimestampEndKey).ValueAsLong();

		GMLInstance instance = new GMLInstance(instanceId, timestampStart, timestampEnd, template, vertexProperties, edgeProperties);

		// load vertex information
		Iterator<KeyValuePair> it_vertices = new KVPIterator(graph_properties_tree, GMLParser.VertexKey);
		while (it_vertices.hasNext()) {
			Iterable<KeyValuePair> vertex_properties_tree = it_vertices.next().ValueAsList();
			long vertexId = GMLParser.getKVPForKey(vertex_properties_tree, GMLParser.VertexIdKey).ValueAsLong();

			// skip vertex if it doesn't exist in template
			if (!template.containsVertex(vertexId)) {
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
				instance.addPropertiesForVertex(vertexPropertiesMap);
			}
		}

		// load edge information
		Iterator<KeyValuePair> it_edges = new KVPIterator(graph_properties_tree, GMLParser.EdgeKey);
		while (it_edges.hasNext()) {
			Iterable<KeyValuePair> edge_properties_tree = it_edges.next().ValueAsList();
			long edgeId = GMLParser.getKVPForKey(edge_properties_tree, GMLParser.EdgeIdKey).ValueAsLong();
			long edgeSource = GMLParser.getKVPForKey(edge_properties_tree, GMLParser.EdgeSourceKey).ValueAsLong();
			long edgeSink = GMLParser.getKVPForKey(edge_properties_tree, GMLParser.EdgeSinkKey).ValueAsLong();

			// skip edge if it doesn't exist in template
			if (!template.containsEdge(edgeId, edgeSource, edgeSink)) {
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
				instance.addPropertiesForEdge(edgePropertiesMap);
			}
		}

		return instance;
	}

	public static Map<Long, GMLInstance> read(InputStream gmlInstance, IPartition partition, PropertySet vertexProperties, PropertySet edgeProperties) throws IOException {
		// read everything in one go for perf reasons, rather than looping over
		// each subgraph

		Iterable<KeyValuePair> gml = GMLParser.parse(gmlInstance);

		Iterable<KeyValuePair> graph_properties_tree = GMLParser.getKVPForKey(gml, GMLParser.GraphKey).ValueAsList();

		long instanceId = GMLParser.getKVPForKey(graph_properties_tree, GMLParser.GraphInstanceIdKey).ValueAsLong();
		long timestampStart = GMLParser.getKVPForKey(graph_properties_tree, GMLParser.GraphInstanceTimestampStartKey).ValueAsLong();
		long timestampEnd = GMLParser.getKVPForKey(graph_properties_tree, GMLParser.GraphInstanceTimestampEndKey).ValueAsLong();

		// define map
		Map<Long, GMLInstance> instances = new HashMap<>(partition.size(), 1f);
		for (ISubgraph subgraph : partition) {
			instances.put(subgraph.getId(), new GMLInstance(instanceId, timestampStart, timestampEnd, subgraph.getTemplate(), vertexProperties, edgeProperties));
		}

		// load vertex information
		Iterator<KeyValuePair> it_vertices = new KVPIterator(graph_properties_tree, GMLParser.VertexKey);
		while (it_vertices.hasNext()) {
			Iterable<KeyValuePair> vertex_properties_tree = it_vertices.next().ValueAsList();
			long vertexId = GMLParser.getKVPForKey(vertex_properties_tree, GMLParser.VertexIdKey).ValueAsLong();

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
				
				instances.get(subgraph.getId()).addPropertiesForVertex(vertexPropertiesMap);
			}
		}

		// load edge information
		Iterator<KeyValuePair> it_edges = new KVPIterator(graph_properties_tree, GMLParser.EdgeKey);
		while (it_edges.hasNext()) {
			Iterable<KeyValuePair> edge_properties_tree = it_edges.next().ValueAsList();
			long edgeId = GMLParser.getKVPForKey(edge_properties_tree, GMLParser.EdgeIdKey).ValueAsLong();
			long edgeSource = GMLParser.getKVPForKey(edge_properties_tree, GMLParser.EdgeSourceKey).ValueAsLong();
			long edgeSink = GMLParser.getKVPForKey(edge_properties_tree, GMLParser.EdgeSinkKey).ValueAsLong();

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
				// add to subgraph containing edge
				for (ISubgraph subgraph : partition) {
					if (subgraph.getTemplate().containsEdge(edgeId, edgeSource, edgeSink)) {
						instances.get(subgraph.getId()).addPropertiesForEdge(edgePropertiesMap);
						break;
					}
				}
			}
		}

		return instances;
	}

	public static void write(ISubgraphInstance instance, OutputStream gmlOutput) throws IOException {
		try (GMLWriter output = new GMLWriter(gmlOutput)) {
			output.writeListOpen(GMLParser.GraphKey);
			output.write(GMLParser.GraphInstanceIdKey, instance.getId());
			output.write(GMLParser.GraphInstanceTimestampStartKey, instance.getTimestampStart());
			output.write(GMLParser.GraphInstanceTimestampEndKey, instance.getTimestampEnd());

			// write vertex values
			for (ISubgraphObjectProperties vertexProperties : instance.getPropertiesForVertices()) {
				Collection<String> properties = vertexProperties.getSpecifiedProperties();
				if (properties.isEmpty()) {
					continue;
				}

				output.writeListOpen(GMLParser.VertexKey);
				output.write(GMLParser.VertexIdKey, vertexProperties.getId());
				for (String property : properties) {
					output.write(KeyValuePair.createKVP(property, GMLWriter.classValueToGMLValue(vertexProperties.getValue(property))));
				}
				output.writeListClose();
			}

			// write edge values - we iterate over edges rather than edges with
			// properties so we can get source/sink information easily
			for (TemplateEdge edge : instance.getTemplate().edges()) {
				ISubgraphObjectProperties edgeProperties = instance.getPropertiesForEdge(edge.getId());
				if (edgeProperties == null) {
					continue;
				}
				Collection<String> properties = edgeProperties.getSpecifiedProperties();
				if (properties.isEmpty()) {
					continue;
				}

				output.writeListOpen(GMLParser.EdgeKey);
				output.write(GMLParser.EdgeIdKey, edge.getId());
				output.write(GMLParser.EdgeSourceKey, edge.getSource().getId());
				output.write(GMLParser.EdgeSinkKey, edge.getSink().getId());
				for (String property : properties) {
					output.write(KeyValuePair.createKVP(property, GMLWriter.classValueToGMLValue(edgeProperties.getValue(property))));
				}
				output.writeListClose();
			}

			output.writeListClose();
		}
	}
}
