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

package edu.usc.goffish.gofs.formats.metis;

import it.unimi.dsi.fastutil.longs.*;

import java.io.*;
import java.util.*;

import edu.usc.goffish.gofs.graph.*;
import edu.usc.goffish.gofs.graph.impl.*;
import edu.usc.goffish.gofs.util.*;

public final class MetisGraph extends AbstractGraph<MetisVertex, MetisEdge> implements IIdentifiableVertexGraph<MetisVertex, MetisEdge> {

	private final Long2ObjectMap<MetisVertex> _vertices;

	private MetisGraph(int initialCapacity) {
		_vertices = new Long2ObjectOpenHashMap<>(initialCapacity);
	}

	public static MetisGraph read(InputStream metisInput) throws IOException {
		if (metisInput == null) {
			throw new IllegalArgumentException();
		}

		try (BufferedReader input = new BufferedReader(new InputStreamReader(metisInput))) {
			String line;

			line = input.readLine();
			@SuppressWarnings("resource")
			long numVertices = new Scanner(line).nextLong();

			MetisGraph graph = new MetisGraph(numVertices > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int)numVertices);

			for (long sourceId = 1; sourceId <= numVertices; sourceId++) {
				line = input.readLine();

				MetisVertex source = graph.getVertex(sourceId);
				if (source == null) {
					source = new MetisVertex(sourceId);
					graph.addVertex(source);
				}

				@SuppressWarnings("resource")
				Scanner s = new Scanner(line);
				while (s.hasNext()) {
					long sinkId = s.nextLong();

					// simple trick to avoid creating edges twice - only create edge when sink >= source
					if (sinkId < sourceId) {
						continue;
					}

					MetisVertex sink = graph.getVertex(sinkId);
					if (sink == null) {
						sink = new MetisVertex(sinkId);
						graph.addVertex(sink);
					}

					graph.connectEdge(new MetisEdge(source, sink));
				}
			}

			return graph;
		} catch (InputMismatchException e) {
			throw new MetisFormatException(e);
		} catch (NoSuchElementException e) {
			throw new MetisFormatException(e);
		}
	}

	public static int write(IIdentifiableVertexGraph<? extends IIdentifiableVertex, ? extends IEdge> graph, OutputStream metisOutput) throws IOException {
		if (graph == null || graph.isDirected()) {
			throw new IllegalArgumentException();
		}
		if (metisOutput == null) {
			throw new IllegalArgumentException();
		}

		// because metis is idiotic, it doesn't like self edge loops. calculate edge count without them
		long edges = 0;
		for (IEdge edge : graph.edges()) {
			if (!edge.getSource().equals(edge.getSink())) {
				edges++;
			}
		}

		try (BufferedWriter output = new BufferedWriter(new OutputStreamWriter(metisOutput))) {
			output.write(Integer.toString(graph.numVertices()));
			output.write(" ");
			output.write(Long.toString(edges));
			output.write(System.lineSeparator());

			int i = 1;
			IIdentifiableVertex vertex = graph.getVertex(i);
			while (vertex != null) {
				for (IEdge edge : vertex.outEdges()) {
					if (edge.getSource().equals(edge.getSink())) {
						// skip self edge loops
						continue;
					}

					output.write(Long.toString(((IIdentifiableVertex)edge.getSink(vertex)).getId()));
					output.write(" ");
				}
				output.write(System.lineSeparator());

				vertex = graph.getVertex(i);
				i++;
			}

			return i - 1;
		}
	}

	public static long[] writeAndRenumber(IIdentifiableVertexGraph<? extends IIdentifiableVertex, ? extends IEdge> graph, OutputStream metisOutput) throws IOException {
		if (graph == null || graph.isDirected()) {
			throw new IllegalArgumentException();
		}
		if (metisOutput == null) {
			throw new IllegalArgumentException();
		}

		// because metis is idiotic, it doesn't like self edge loops. calculate edge count without them
		long edges = 0;
		for (IEdge edge : graph.edges()) {
			if (!edge.getSource().equals(edge.getSink())) {
				edges++;
			}
		}

		try (BufferedWriter output = new BufferedWriter(new OutputStreamWriter(metisOutput))) {
			output.write(Integer.toString(graph.numVertices()));
			output.write(" ");
			output.write(Long.toString(edges));
			output.write(System.lineSeparator());

			long[] renumbering = new long[graph.numVertices() + 1];
			Map<Long, Integer> rRenumbering = new HashMap<>(renumbering.length, 1f);
			renumbering[0] = renumbering.length - 1;

			// create renumbering mappings to translate from real id to metis id and vice versa
			int i = 1;
			for (IIdentifiableVertex vertex : graph.vertices()) {
				renumbering[i] = vertex.getId();
				rRenumbering.put(renumbering[i], i);
				i++;
			}

			// use mappings to write out metis graph
			i = 1;
			while (i < renumbering.length) {
				IIdentifiableVertex vertex = graph.getVertex(renumbering[i]);
				for (IEdge edge : vertex.outEdges()) {
					if (edge.getSource().equals(edge.getSink())) {
						// skip self edge loops
						continue;
					}

					long sinkId = ((IIdentifiableVertex)edge.getSink(vertex)).getId();
					output.write(rRenumbering.get(sinkId).toString());
					output.write(" ");
				}
				output.write(System.lineSeparator());

				i++;
			}

			return renumbering;
		}
	}

	@Override
	public MetisVertex getVertex(long vertexId) {
		return _vertices.get(vertexId);
	}

	@Override
	public boolean containsVertex(long vertexId) {
		return _vertices.containsKey(vertexId);
	}

	@Override
	public boolean isDirected() {
		return false;
	}

	@Override
	public Iterable<MetisVertex> vertices() {
		return IterableUtils.unmodifiableIterable(_vertices.values());
	}

	@Override
	public Iterable<MetisEdge> edges() {
		return UniqueEdgeIterable.fromTypedVertices(vertices());
	}

	@Override
	public int numVertices() {
		return _vertices.size();
	}

	@Override
	public boolean containsVertex(IVertex vertex) {
		if (vertex == null) {
			throw new IllegalArgumentException();
		}

		if (vertex instanceof MetisVertex) {
			return containsVertex((MetisVertex)vertex);
		}

		return false;
	}

	public boolean containsVertex(MetisVertex vertex) {
		if (vertex == null) {
			throw new IllegalArgumentException();
		}

		return _vertices.containsKey(vertex.getId());
	}

	@Override
	public boolean containsEdge(IEdge edge) {
		if (edge == null) {
			throw new IllegalArgumentException();
		}

		if (containsVertex(edge.getSink()) && containsVertex(edge.getSource())) {
			return edge.getSource().containsOutEdgeTo(edge, edge.getSink()) && edge.getSink().containsOutEdgeTo(edge, edge.getSource());
		}

		return false;
	}

	private boolean addVertex(MetisVertex vertex) {
		if (vertex == null) {
			throw new IllegalArgumentException();
		}
		if (vertex.getId() < 1) {
			throw new IllegalArgumentException();
		}

		if (!_vertices.containsKey(vertex.getId())) {
			_vertices.put(vertex.getId(), vertex);
			return true;
		}

		return false;
	}

	private void connectEdge(MetisEdge edge) {
		if (edge == null) {
			throw new IllegalArgumentException();
		}
		if (!containsVertex(edge.getSource()) || !containsVertex(edge.getSink())) {
			throw new IllegalArgumentException();
		}
		if (edge.getSource().containsOutEdgeTo(edge, edge.getSink())) {
			throw new IllegalArgumentException();
		}

		if (edge.getSource().equals(edge.getSink())) {
			// self edge, only add once
			edge.getSource().addOutEdge(edge);
		} else {
			edge.getSource().addOutEdge(edge);
			edge.getSink().addOutEdge(edge);
		}
	}

	@Override
	public String toString() {
		return _vertices.values().toString();
	}
}
