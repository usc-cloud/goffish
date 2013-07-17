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

package edu.usc.goffish.gofs.partition.gml;

import it.unimi.dsi.fastutil.longs.*;

import java.io.*;
import java.util.*;

import edu.usc.goffish.gofs.*;
import edu.usc.goffish.gofs.formats.gml.*;
import edu.usc.goffish.gofs.graph.*;
import edu.usc.goffish.gofs.partition.*;
import edu.usc.goffish.gofs.util.*;

public class GMLPartition extends BasePartition implements ISerializablePartition {

	private final PropertySet _vertexProperties;
	private final PropertySet _edgeProperties;
	private final Iterable<InputStream> _gmlInstanceStreams;

	private GMLPartition(int id, IGraph<? extends TemplateVertex, ? extends TemplateEdge> partitionGraph, PropertySet vertexProperties, PropertySet edgeProperties, Long2IntMap remoteVertices, Iterable<InputStream> gmlInstanceStreams) {
		super(id, partitionGraph, vertexProperties, edgeProperties, remoteVertices, new GMLSubgraphFactory());

		_vertexProperties = vertexProperties;
		_edgeProperties = edgeProperties;
		_gmlInstanceStreams = gmlInstanceStreams;
	}

	/**
	 * Creates a partition backed by the specified GML data. The template is
	 * loaded into memory, the instances are loaded on demand. The resulting
	 * subgraphs contained by this partition do not support querying for
	 * instance data, as GML files are not sufficient for this query. Rather the
	 * expected use case is that this partition will be written to slices
	 * through the SliceManager, the SliceManager will be used to load the
	 * partition from slices, and that partition may be queried for instance
	 * data.
	 * 
	 * @param id
	 *            the id of the partition to create
	 * @param gmlTemplateStream
	 *            an input stream containing the GML template
	 * @param gmlInstanceStreams
	 *            an Iterable of input streams for each GML instance file
	 * @return a partition constructed from the GML template, and backed by the
	 *         GML instances
	 */
	public static GMLPartition parseGML(int id, InputStream gmlTemplateStream, Iterable<InputStream> gmlInstanceStreams) throws IOException {
		if (gmlTemplateStream == null) {
			throw new IllegalArgumentException();
		}
		if (gmlInstanceStreams == null) {
			throw new IllegalArgumentException();
		}

		GMLGraph template = GMLGraph.read(gmlTemplateStream);
		return new GMLPartition(id, template.getTemplate(), template.getVertexProperties(), template.getEdgeProperties(), template.getRemoteVertexMappings(), gmlInstanceStreams);
	}

	@Override
	public Iterable<List<Map<Long, ? extends ISubgraphInstance>>> getSubgraphsInstances() {
		return new Iterable<List<Map<Long, ? extends ISubgraphInstance>>>() {
			@Override
			public Iterator<List<Map<Long, ? extends ISubgraphInstance>>> iterator() {
				return new GMLInstanceIterator(_gmlInstanceStreams, _vertexProperties, _edgeProperties);
			}
		};
	}

	private class GMLInstanceIterator extends AbstractWrapperIterator<List<Map<Long, ? extends ISubgraphInstance>>> implements Iterator<List<Map<Long, ? extends ISubgraphInstance>>> {

		private final Iterator<InputStream> _itStreams;
		private final int _grouping;
		private final PropertySet _vertexProperties;
		private final PropertySet _edgeProperties;

		public GMLInstanceIterator(Iterable<InputStream> gmlInstanceStreams, PropertySet vertexProperties, PropertySet edgeProperties) {
			this(gmlInstanceStreams, 1, vertexProperties, edgeProperties);
		}
		
		public GMLInstanceIterator(Iterable<InputStream> gmlInstanceStreams, int grouping, PropertySet vertexProperties, PropertySet edgeProperties) {
			if (gmlInstanceStreams == null) {
				throw new IllegalArgumentException();
			}
			if (grouping < 1) {
				throw new IllegalArgumentException();
			}
			if (vertexProperties == null) {
				throw new IllegalArgumentException();
			}
			if (edgeProperties == null) {
				throw new IllegalArgumentException();
			}
			
			_itStreams = gmlInstanceStreams.iterator();
			_grouping = grouping;
			_vertexProperties = vertexProperties;
			_edgeProperties = edgeProperties;
		}

		@Override
		protected List<Map<Long, ? extends ISubgraphInstance>> advanceToNext() {
			if (!_itStreams.hasNext()) {
				return null;
			}

			ArrayList<Map<Long, ? extends ISubgraphInstance>> instancesList = new ArrayList<>(_grouping);
			for (int i = 0; i < _grouping && _itStreams.hasNext(); i++) {
				try {
					instancesList.add(GMLInstance.read(_itStreams.next(), GMLPartition.this, _vertexProperties, _edgeProperties));
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			}

			return instancesList;
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}
	}
}
