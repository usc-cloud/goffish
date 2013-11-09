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
import edu.usc.goffish.gofs.partition.components.*;
import edu.usc.goffish.gofs.util.*;

public class GMLPartition extends Partition implements ISerializablePartition {

	private final Iterable<InputStream> _gmlInstanceStreams;

	protected GMLPartition(int id, boolean directed, Collection<? extends ISubgraph> subgraphs, PropertySet vertexProperties, PropertySet edgeProperties, Iterable<InputStream> gmlInstanceStreams) {
		super(id, directed, subgraphs, vertexProperties, edgeProperties);

		if (gmlInstanceStreams == null) {
			throw new IllegalArgumentException();
		}

		_gmlInstanceStreams = gmlInstanceStreams;
	}

	/**
	 * Creates a partition backed by the specified GML data. The template is loaded into memory, the instances are
	 * loaded on demand. The resulting subgraphs contained by this partition do not support querying for instance data,
	 * as GML files are not sufficient for this query. Rather the expected use case is that this partition will be
	 * written to slices through the SliceManager, the SliceManager will be used to load the partition from slices, and
	 * that partition may be queried for instance data.
	 * 
	 * @param partitionId
	 *            the id of the partition to create
	 * @param graphComponentizer
	 *            the componentizer to use to divide this graph into subgraphs
	 * @param gmlTemplateStream
	 *            an input stream containing the GML template
	 * @param gmlInstanceStreams
	 *            an Iterable of input streams for each GML instance file
	 * @return a partition constructed from the GML template, and backed by the GML instances
	 */
	public static GMLPartition parseGML(int partitionId, IGraphComponentizer graphComponentizer, InputStream gmlTemplateStream, Iterable<InputStream> gmlInstanceStreams) throws IOException {
		if (gmlTemplateStream == null) {
			throw new IllegalArgumentException();
		}
		if (gmlInstanceStreams == null) {
			throw new IllegalArgumentException();
		}

		GMLGraph graph = GMLGraph.read(gmlTemplateStream);

		// split into components
		Collection<? extends Collection<TemplateVertex>> components = graphComponentizer.componentize(graph.getTemplate());

		// create subgraphs from components
		ArrayList<ISubgraph> subgraphs = new ArrayList<>(components.size());
		int subgraphCount = 0;
		for (Collection<TemplateVertex> component : components) {
			TemplateGraph template = new TemplateGraph(graph.isDirected());
			for (TemplateVertex vertex : component) {
				template.addVertex(vertex);
			}

			long subgraphId = subgraphCount++ | (((long)partitionId) << 32);
			subgraphs.add(new GMLGraph(subgraphId, template, graph.getVertexProperties(), graph.getEdgeProperties()));
		}

		return new GMLPartition(partitionId, graph.isDirected(), subgraphs, graph.getVertexProperties(), graph.getEdgeProperties(), gmlInstanceStreams);
	}

	@Override
	public Iterable<Long2ObjectMap<? extends ISubgraphInstance>> getSubgraphsInstances() {
		return new Iterable<Long2ObjectMap<? extends ISubgraphInstance>>() {
			@Override
			public Iterator<Long2ObjectMap<? extends ISubgraphInstance>> iterator() {
				return new GMLInstanceIterator(_gmlInstanceStreams, getVertexProperties(), getEdgeProperties());
			}
		};
	}

	private class GMLInstanceIterator extends AbstractWrapperIterator<Long2ObjectMap<? extends ISubgraphInstance>> {

		private final Iterator<InputStream> _itStreams;
		private final PropertySet _vertexProperties;
		private final PropertySet _edgeProperties;

		public GMLInstanceIterator(Iterable<InputStream> gmlInstanceStreams, PropertySet vertexProperties, PropertySet edgeProperties) {
			if (gmlInstanceStreams == null) {
				throw new IllegalArgumentException();
			}
			if (vertexProperties == null) {
				throw new IllegalArgumentException();
			}
			if (edgeProperties == null) {
				throw new IllegalArgumentException();
			}

			_itStreams = gmlInstanceStreams.iterator();
			_vertexProperties = vertexProperties;
			_edgeProperties = edgeProperties;
		}

		@Override
		protected Long2ObjectMap<? extends ISubgraphInstance> advanceToNext() {
			if (!_itStreams.hasNext()) {
				return null;
			}

			try {
				return GMLInstance.read(_itStreams.next(), GMLPartition.this, _vertexProperties, _edgeProperties);
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
	}
}
