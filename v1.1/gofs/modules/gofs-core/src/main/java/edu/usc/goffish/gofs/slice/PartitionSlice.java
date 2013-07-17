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

package edu.usc.goffish.gofs.slice;

import java.io.*;
import java.util.*;

import com.esotericsoftware.kryo.*;
import com.esotericsoftware.kryo.io.*;

import edu.usc.goffish.gofs.*;
import edu.usc.goffish.gofs.partition.*;

class PartitionSlice implements ISlice {

	private static final long serialVersionUID = 1L;

	private transient final UUID _id;

	public final boolean IsDirected;
	public final Collection<SubgraphTuple> Subgraphs;
	public final Collection<PropertyTuple> VertexProperties;
	public final Collection<PropertyTuple> EdgeProperties;

	private transient Collection<ISubgraph> _subgraphsCache;
	private transient PropertySet _vertexPropertiesCache;
	private transient PropertySet _edgePropertiesCache;
	
	public PartitionSlice(IPartition partition) {
		if (partition == null) {
			throw new IllegalArgumentException();
		}

		_id = UUID.randomUUID();
		_subgraphsCache = null;
		_vertexPropertiesCache = null;
		_edgePropertiesCache = null;
		
		IsDirected = partition.isDirected();

		// load subgraphs
		ArrayList<SubgraphTuple> subgraphs = new ArrayList<>(partition.size());
		for (ISubgraph subgraph : partition) {
			// load vertices
			ArrayList<VertexTuple> vertices = new ArrayList<>(subgraph.numVertices());
			for (TemplateVertex vertex : subgraph.vertices()) {
				// load edges
				ArrayList<EdgeTuple> edges = new ArrayList<>(vertex.outDegree());
				for (TemplateEdge edge : vertex.outEdgesUnique()) {
					edges.add(new EdgeTuple(edge.getId(), edge.getSink().getId()));
				}
				
				vertices.add(new VertexTuple(vertex.getId(), Collections.unmodifiableList(edges)));
			}

			// TODO: we need to add kryo serializer for fastutils collections, until then, copy into java collection
			subgraphs.add(new SubgraphTuple(subgraph.getId(), Collections.unmodifiableList(vertices), Collections.unmodifiableMap(new HashMap<>(subgraph.getRemoteVertexMappings()))));
		}
		Subgraphs = Collections.unmodifiableList(subgraphs);
		
		// load vertex properties
		ArrayList<PropertyTuple> vertexProperties = new ArrayList<>(partition.getVertexProperties().size());
		for (Property property : partition.getVertexProperties()) {
			// TODO: we need to add kryo serializer for fastutils collections, until then, copy into java collection
			vertexProperties.add(new PropertyTuple(property.getName(), property.getType(), property.isStatic(), Collections.unmodifiableMap(new HashMap<>(property.getDefaults()))));
		}
		VertexProperties = Collections.unmodifiableList(vertexProperties);

		// load edge properties
		ArrayList<PropertyTuple> edgeProperties = new ArrayList<>(partition.getEdgeProperties().size());
		for (Property property : partition.getEdgeProperties()) {
			// TODO: we need to add kryo serializer for fastutils collections, until then, copy into java collection
			edgeProperties.add(new PropertyTuple(property.getName(), property.getType(), property.isStatic(), Collections.unmodifiableMap(new HashMap<>(property.getDefaults()))));
		}
		EdgeProperties = Collections.unmodifiableList(edgeProperties);
	}

	@Override
	public UUID getId() {
		return _id;
	}
	
	public Collection<ISubgraph> getSubgraphs(ISubgraphFactory factory) {
		if (_subgraphsCache == null) {
			_subgraphsCache = new ArrayList<>(Subgraphs.size());
			for (SubgraphTuple subgraph : Subgraphs) {
				
				// recreate template
				TemplateGraph template = new TemplateGraph(IsDirected, subgraph.AdjacencyList.size());
				for (VertexTuple vertex : subgraph.AdjacencyList) {
					
					// retrieve source
					TemplateVertex source = template.getVertex(vertex.VertexId);
					if (source == null) {
						source = new TemplateVertex(vertex.VertexId, subgraph.RemoteVertices.containsKey(vertex.VertexId));
						template.addVertex(source);
					}
					
					for (EdgeTuple edge : vertex.Edges) {
						
						// retrieve sink
						TemplateVertex sink = template.getVertex(edge.EdgeSink);
						if (sink == null) {
							sink = new TemplateVertex(edge.EdgeSink, subgraph.RemoteVertices.containsKey(edge.EdgeSink));
							template.addVertex(sink);
						}
						
						template.connectEdge(new TemplateEdge(edge.EdgeId, source, sink));
					}
				}
				
				// create subgraph
				_subgraphsCache.add(factory.createSubgraph(subgraph.SubgraphId, template, getVertexProperties(), getEdgeProperties(), subgraph.RemoteVertices));
			}
		}
		
		return _subgraphsCache;
	}
	
	public PropertySet getVertexProperties() {
		if (_vertexPropertiesCache == null) {
			ArrayList<Property> properties = new ArrayList<>(VertexProperties.size());
			for (PropertyTuple property : VertexProperties) {
				properties.add(new Property(property.Name, property.Type, property.IsStatic, property.DefaultValues));
			}
			
			_vertexPropertiesCache = new PropertySet(properties);
		}
		
		return _vertexPropertiesCache;
	}
	
	public PropertySet getEdgeProperties() {
		if (_edgePropertiesCache == null) {
			ArrayList<Property> properties = new ArrayList<>(EdgeProperties.size());
			for (PropertyTuple property : EdgeProperties) {
				properties.add(new Property(property.Name, property.Type, property.IsStatic, property.DefaultValues));
			}
			
			_edgePropertiesCache = new PropertySet(properties);
		}
		
		return _edgePropertiesCache;
	}
	
	static class SubgraphTuple implements Serializable {
		
		private static final long serialVersionUID = 1L;
		
		public final long SubgraphId;
		public final Collection<VertexTuple> AdjacencyList;
		public final Map<Long, Integer> RemoteVertices;
		
		protected SubgraphTuple(long subgraphId, List<VertexTuple> adjacencyList, Map<Long, Integer> remoteVertices) {
			SubgraphId = subgraphId;
			AdjacencyList = adjacencyList;
			RemoteVertices = remoteVertices;
		}
	}
	
	static class VertexTuple implements Serializable {
		
		private static final long serialVersionUID = 1L;
		
		public final long VertexId;
		public final Collection<EdgeTuple> Edges;
		
		protected VertexTuple(long vertexId, List<EdgeTuple> edges) {
			VertexId = vertexId;
			Edges = edges;
		}
	}
	
	static class EdgeTuple implements Serializable {
		
		private static final long serialVersionUID = 1L;
		
		public final long EdgeId;
		public final long EdgeSink;
		
		protected EdgeTuple(long edgeId, long edgeSink) {
			EdgeId = edgeId;
			EdgeSink = edgeSink;
		}
	}
	
	static final class PropertyTuple implements Serializable {

		private static final long serialVersionUID = 1L;

		@SuppressWarnings("rawtypes")
		private static final Class<? extends Map> ConcreteDefaultValuesType = Collections.unmodifiableMap(new HashMap<Void, Void>()).getClass();
		
		public final String Name;
		public final Class<? extends Object> Type;
		public final boolean IsStatic;
		public final Map<Long, Object> DefaultValues;

		protected PropertyTuple(String name, Class<?> type, boolean isStatic, Map<Long, Object> defaultValues) {
			if (!ConcreteDefaultValuesType.isAssignableFrom(defaultValues.getClass())) {
				// concrete instance type must match type used for deserialization in PropertyTupleSerializer below
				throw new IllegalStateException();
			}
			
			Name = name;
			Type = type;
			IsStatic = isStatic;
			DefaultValues = defaultValues;
		}
		
		static class PropertyTupleSerializer extends Serializer<PartitionSlice.PropertyTuple> {

			public PropertyTupleSerializer() {
				super(true, true);
			}
			
			@SuppressWarnings("unchecked")
			@Override
			public PartitionSlice.PropertyTuple read(Kryo kryo, Input input, Class<PartitionSlice.PropertyTuple> type) {
				try {
					return new PartitionSlice.PropertyTuple(input.readString(), Class.forName(input.readString()), input.readBoolean(), kryo.readObject(input, ConcreteDefaultValuesType));
				} catch (ClassNotFoundException e) {
					throw new RuntimeException(e);
				}
			}

			@Override
			public void write(Kryo kryo, Output output, PartitionSlice.PropertyTuple object) {
				output.writeString(object.Name);
				output.writeString(object.Type.getName());
				output.writeBoolean(object.IsStatic);
				kryo.writeObject(output, object.DefaultValues);
			}
		}
	}
}
