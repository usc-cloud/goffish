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

package edu.usc.goffish.gofs.slice;

import it.unimi.dsi.fastutil.longs.*;

import java.io.*;
import java.util.*;

import com.esotericsoftware.kryo.*;
import com.esotericsoftware.kryo.io.*;

import edu.usc.goffish.gofs.*;
import edu.usc.goffish.gofs.partition.*;

final class PartitionSlice implements ISlice {

	private static final long serialVersionUID = 3337424186075764336L;

	private transient final UUID _id;

	private final boolean _isDirected;
	@NotNull
	private final ArrayList<SubgraphTuple> _subgraphs;
	@NotNull
	private final ArrayList<PropertyTuple> _vertexProperties;
	@NotNull
	private final ArrayList<PropertyTuple> _edgeProperties;

	private transient PropertySet _vertexPropertiesCache;
	private transient PropertySet _edgePropertiesCache;

	PartitionSlice(IPartition partition) {
		this(partition, null);
	}

	PartitionSlice(IPartition partition, Iterable<? extends Collection<ISubgraph>> alternateSubgraphOrdering) {
		if (partition == null) {
			throw new IllegalArgumentException();
		}
		if (alternateSubgraphOrdering == null) {
			alternateSubgraphOrdering = Collections.singletonList(partition);
		}

		_id = UUID.randomUUID();
		_vertexPropertiesCache = null;
		_edgePropertiesCache = null;

		_isDirected = partition.isDirected();

		// load subgraphs, ordering to take advantage of the subgraph bins and reduce slice reads
		_subgraphs = new ArrayList<>(partition.size());
		for (Collection<ISubgraph> subgraphBin : alternateSubgraphOrdering) {
			for (ISubgraph subgraph : subgraphBin) {
				Long2ObjectMap<RemoteVertexInfo> remoteVertices = new Long2ObjectAVLTreeMap<>();

				// load vertices
				ArrayList<VertexTuple> vertices = new ArrayList<>(subgraph.numVertices());
				for (ITemplateVertex vertex : subgraph.vertices()) {
					// load edges
					ArrayList<EdgeTuple> edges = new ArrayList<>(vertex.outDegree());
					for (ITemplateEdge edge : vertex.outEdgesUnique()) {
						edges.add(new EdgeTuple(edge.getId(), edge.getSink().getId()));
					}
					edges.trimToSize();

					vertices.add(new VertexTuple(vertex.getId(), edges));

					// load remote vertices
					if (vertex.isRemote()) {
						remoteVertices.put(vertex.getId(), new RemoteVertexInfo(vertex.getRemotePartitionId(), vertex.getRemoteSubgraphId()));
					}
				}

				_subgraphs.add(new SubgraphTuple(subgraph.getId(), vertices, remoteVertices));
			}
		}

		// load vertex properties
		_vertexProperties = new ArrayList<>(partition.getVertexProperties().size());
		for (Property property : partition.getVertexProperties()) {
			_vertexProperties.add(new PropertyTuple(property.getName(), property.getType(), property.isStatic(), property.getDefaults()));
		}

		// load edge properties
		_edgeProperties = new ArrayList<>(partition.getEdgeProperties().size());
		for (Property property : partition.getEdgeProperties()) {
			_edgeProperties.add(new PropertyTuple(property.getName(), property.getType(), property.isStatic(), property.getDefaults()));
		}
	}

	@Override
	public UUID getId() {
		return _id;
	}

	public boolean isDirected() {
		return _isDirected;
	}

	public Collection<ISubgraph> buildSubgraphs(SliceManager sliceManager) {
		ArrayList<ISubgraph> subgraphs = new ArrayList<>(_subgraphs.size());
		for (int i = 0; i < _subgraphs.size(); i++) {
			SubgraphTuple subgraph = _subgraphs.get(i);

			// recreate template
			TemplateGraph template = new TemplateGraph(_isDirected, subgraph.AdjacencyList.size());
			for (int j = 0; j < subgraph.AdjacencyList.size(); j++) {
				VertexTuple vertex = subgraph.AdjacencyList.get(j);

				// retrieve source
				TemplateVertex source = template.getVertex(vertex.VertexId);
				if (source == null) {
					RemoteVertexInfo rvi = subgraph.RemoteVertices.get(vertex.VertexId);
					if (rvi != null) {
						source = new TemplateRemoteVertex(vertex.VertexId, rvi.RemotePartition, rvi.RemoteSubgraph);
					} else {
						source = new TemplateVertex(vertex.VertexId);
					}
					template.addVertex(source);
				}

				for (int k = 0; k < vertex.Edges.size(); k++) {
					EdgeTuple edge = vertex.Edges.get(k);

					// retrieve sink
					TemplateVertex sink = template.getVertex(edge.EdgeSink);
					if (sink == null) {
						RemoteVertexInfo rvi = subgraph.RemoteVertices.get(edge.EdgeSink);
						if (rvi != null) {
							sink = new TemplateRemoteVertex(edge.EdgeSink, rvi.RemotePartition, rvi.RemoteSubgraph);
						} else {
							sink = new TemplateVertex(edge.EdgeSink);
						}
						template.addVertex(sink);
					}

					template.connectEdge(new TemplateEdge(edge.EdgeId, source, sink));
				}
			}

			// create subgraph
			subgraphs.add(new SliceSubgraph(subgraph.SubgraphId, template, sliceManager, getVertexProperties(), getEdgeProperties()));
		}

		return subgraphs;
	}

	public PropertySet getVertexProperties() {
		if (_vertexPropertiesCache == null) {
			ArrayList<Property> properties = new ArrayList<>(_vertexProperties.size());
			for (PropertyTuple property : _vertexProperties) {
				properties.add(new Property(property.Name, property.Type, property.IsStatic, property.DefaultValues, false));
			}

			_vertexPropertiesCache = new PropertySet(properties);
		}

		return _vertexPropertiesCache;
	}

	public PropertySet getEdgeProperties() {
		if (_edgePropertiesCache == null) {
			ArrayList<Property> properties = new ArrayList<>(_edgeProperties.size());
			for (PropertyTuple property : _edgeProperties) {
				properties.add(new Property(property.Name, property.Type, property.IsStatic, property.DefaultValues, false));
			}

			_edgePropertiesCache = new PropertySet(properties);
		}

		return _edgePropertiesCache;
	}

	static final class SubgraphTuple implements Serializable {

		private static final long serialVersionUID = -144175409675902319L;

		public final long SubgraphId;
		@NotNull
		public final ArrayList<VertexTuple> AdjacencyList;
		@NotNull
		public final Long2ObjectMap<RemoteVertexInfo> RemoteVertices;

		protected SubgraphTuple(long subgraphId, ArrayList<VertexTuple> adjacencyList, Long2ObjectMap<RemoteVertexInfo> remoteVertices) {
			SubgraphId = subgraphId;
			AdjacencyList = adjacencyList;
			RemoteVertices = remoteVertices;
		}
	}

	static final class RemoteVertexInfo implements Serializable {

		private static final long serialVersionUID = -3814205020357442820L;

		public final int RemotePartition;
		public final long RemoteSubgraph;

		protected RemoteVertexInfo(int remotePartition, long remoteSubgraph) {
			RemotePartition = remotePartition;
			RemoteSubgraph = remoteSubgraph;
		}
	}

	static final class VertexTuple implements Serializable {

		private static final long serialVersionUID = 286996756646877514L;

		public final long VertexId;
		@NotNull
		public final ArrayList<EdgeTuple> Edges;

		protected VertexTuple(long vertexId, ArrayList<EdgeTuple> edges) {
			VertexId = vertexId;
			Edges = edges;
		}
	}

	static final class EdgeTuple implements Serializable {

		private static final long serialVersionUID = 270607272373432013L;

		public final long EdgeId;
		public final long EdgeSink;

		protected EdgeTuple(long edgeId, long edgeSink) {
			EdgeId = edgeId;
			EdgeSink = edgeSink;
		}
	}

	static final class PropertyTuple implements Serializable {

		private static final long serialVersionUID = 8425656871041400209L;

		@NotNull
		public final String Name;
		@NotNull
		public final Class<? extends Object> Type;
		public final boolean IsStatic;
		@NotNull
		public final Long2ObjectMap<Object> DefaultValues;

		protected PropertyTuple(String name, Class<?> type, boolean isStatic, Long2ObjectMap<Object> defaultValues) {
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
					return new PartitionSlice.PropertyTuple(input.readString(), Class.forName(input.readString()), input.readBoolean(), (Long2ObjectMap<Object>)kryo.readClassAndObject(input));
				} catch (ClassNotFoundException e) {
					throw new RuntimeException(e);
				}
			}

			@Override
			public void write(Kryo kryo, Output output, PartitionSlice.PropertyTuple object) {
				output.writeAscii(object.Name);
				output.writeAscii(object.Type.getName());
				output.writeBoolean(object.IsStatic);
				kryo.writeClassAndObject(output, object.DefaultValues);
			}
		}
	}
}
