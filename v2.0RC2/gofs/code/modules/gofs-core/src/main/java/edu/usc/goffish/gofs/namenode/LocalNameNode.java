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

package edu.usc.goffish.gofs.namenode;

import it.unimi.dsi.fastutil.ints.*;

import java.io.*;
import java.net.*;
import java.util.*;

import edu.usc.goffish.gofs.*;
import edu.usc.goffish.gofs.partition.*;
import edu.usc.goffish.gofs.slice.*;

/**
 * This class is an in-memory instance of INameNode. It is used as the basis for many other INameNode implementations.
 * This class is fully thread safe.
 */
public class LocalNameNode implements IInternalNameNode, IPartitionDirectory, Serializable {

	private static final long serialVersionUID = 1L;

	private final SortedSet<URI> _dataNodes;
	private Class<? extends ISliceSerializer> _sliceSerializer;
	private final HashMap<String, Int2ObjectMap<URI>> _sliceMappings;

	public LocalNameNode() {
		_dataNodes = new TreeSet<>();
		_sliceSerializer = null;
		_sliceMappings = new HashMap<>();
	}

	public LocalNameNode(URI uri) {
		this();
	}

	/**
	 * Writes a human-readable version of the state of this name node to the given output stream. Meant to be used in
	 * conjunction with {@link #readReadable(InputStream)} for debugging purposes.
	 * 
	 * @param nameNode
	 *            the name node to write out
	 * @param outputStream
	 *            the stream to write the name node to
	 * @throws IOException
	 */
	public static void writeReadable(LocalNameNode nameNode, OutputStream outputStream) throws IOException {
		OutputStreamWriter output = new OutputStreamWriter(outputStream);
		synchronized (nameNode) {

			output.write(Integer.toString(nameNode._dataNodes.size()) + System.lineSeparator());
			for (URI dataNode : nameNode._dataNodes) {
				output.write(dataNode + System.lineSeparator());
			}

			if (nameNode._sliceSerializer != null) {
				output.write(nameNode._sliceSerializer.getName());
			}
			output.write(System.lineSeparator());

			for (Map.Entry<String, Int2ObjectMap<URI>> entry1 : nameNode._sliceMappings.entrySet()) {
				for (Int2ObjectMap.Entry<URI> entry2 : entry1.getValue().int2ObjectEntrySet()) {
					output.write(entry1.getKey() + "->" + entry2.getIntKey() + "->" + entry2.getValue() + System.lineSeparator());
				}
			}
		}
		output.flush();
	}

	/**
	 * Reads a human-readable version of the state of this name node from the given input stream. Meant to be used in
	 * conjunction with {@link #writeReadable(LocalNameNode, OutputStream)} for debugging purposes.
	 * 
	 * @param inputStream
	 *            the stream to read the name node from
	 * @return a name node instance as read from the stream
	 * @throws IOException
	 */
	public static LocalNameNode readReadable(InputStream inputStream) throws IOException {
		LocalNameNode tmp = new LocalNameNode();

		BufferedReader input = new BufferedReader(new InputStreamReader(inputStream));
		String line = input.readLine();

		int numDataNodes = Integer.parseInt(line);
		for (int i = 0; i < numDataNodes; i++) {
			tmp.addDataNode(URI.create(input.readLine()));
		}

		String serializer = input.readLine();
		if (!serializer.isEmpty()) {
			tmp.setSerializer(serializer);
		}

		line = input.readLine();
		while (line != null) {
			String[] splits = line.split("->");
			tmp.putPartitionMapping(splits[0], Integer.parseInt(splits[1]), URI.create(splits[2]));
			line = input.readLine();
		}

		return tmp;
	}

	/**
	 * Resets the state of this name node with the state of the other name node.
	 * 
	 * @param other
	 *            the name node to use to reset the state
	 */
	public synchronized void clearAndPutAll(LocalNameNode other) {
		if (other == null) {
			throw new IllegalArgumentException();
		}

		_dataNodes.clear();
		_dataNodes.addAll(other._dataNodes);

		_sliceSerializer = other._sliceSerializer;

		_sliceMappings.clear();
		for (Map.Entry<String, Int2ObjectMap<URI>> entry : other._sliceMappings.entrySet()) {
			_sliceMappings.put(entry.getKey(), new Int2ObjectOpenHashMap<>(entry.getValue()));
		}
	}

	@Override
	public URI getURI() {
		return null;
	}

	@Override
	public boolean isAvailable() {
		return true;
	}

	@Override
	public synchronized void addDataNode(URI dataNode) {
		if (dataNode == null || dataNode.isOpaque() || !dataNode.isAbsolute() || !dataNode.getPath().endsWith("/")) {
			throw new IllegalArgumentException();
		}

		_dataNodes.add(dataNode);
	}

	@Override
	public synchronized Set<URI> getDataNodes() {
		return new HashSet<URI>(_dataNodes);
	}

	@Override
	public synchronized void clearDataNodes() {
		_dataNodes.clear();
	}

	/**
	 * Helper method to allow for setting serializer type through a string.
	 * 
	 * @param sliceSerializerType
	 *            class name of serializer type
	 */
	public synchronized void setSerializer(String sliceSerializerType) {
		if (sliceSerializerType == null) {
			throw new IllegalArgumentException();
		}

		try {
			setSerializer(SliceSerializerProvider.loadSliceSerializerType(sliceSerializerType));
		} catch (ReflectiveOperationException e) {
			throw new IllegalArgumentException(e);
		}
	}

	@Override
	public synchronized void setSerializer(Class<? extends ISliceSerializer> sliceSerializer) {
		if (sliceSerializer == null) {
			throw new IllegalArgumentException();
		}

		try {
			sliceSerializer.getConstructor();
		} catch (ReflectiveOperationException e) {
			throw new IllegalArgumentException(e);
		}

		_sliceSerializer = sliceSerializer;
	}

	@Override
	public synchronized ISliceSerializer getSerializer() {
		if (_sliceSerializer == null) {
			return null;
		}

		try {
			return SliceSerializerProvider.loadSliceSerializer(_sliceSerializer);
		} catch (ReflectiveOperationException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Returns the type of the current serializer.
	 * 
	 * @return the type of the current serializer
	 */
	public synchronized Class<? extends ISliceSerializer> getSerializerType() {
		return _sliceSerializer;
	}

	@Override
	public IPartitionDirectory getPartitionDirectory() {
		return this;
	}

	@Override
	public synchronized Collection<String> getGraphs() {
		return new ArrayList<>(_sliceMappings.keySet());
	}

	@Override
	public synchronized IntCollection getPartitions(String graphId) {
		Int2ObjectMap<URI> partitionMap = _sliceMappings.get(graphId);
		if (partitionMap == null) {
			return null;
		}

		return new IntArrayList(partitionMap.keySet());
	}

	@Override
	public synchronized IntCollection getMatchingPartitions(String graphId, URI locationToMatch) {
		if (graphId == null) {
			throw new IllegalArgumentException();
		}
		if (locationToMatch == null || locationToMatch.isOpaque()) {
			throw new IllegalArgumentException();
		}

		if (!_sliceMappings.containsKey(graphId)) {
			return IntLists.EMPTY_LIST;
		}

		locationToMatch = locationToMatch.normalize();

		IntArrayList matchingPartitions = new IntArrayList(_sliceMappings.get(graphId).size());
		for (Int2ObjectMap.Entry<URI> entry : _sliceMappings.get(graphId).int2ObjectEntrySet()) {
			if (locationToMatch.relativize(entry.getValue()) != entry.getValue()) {
				matchingPartitions.add(entry.getIntKey());
			}
		}

		return matchingPartitions;
	}

	@Override
	public synchronized URI getPartitionMapping(String graphId, int partitionId) {
		if (graphId == null) {
			throw new IllegalArgumentException();
		}
		if (partitionId == Partition.INVALID_PARTITION) {
			throw new IllegalArgumentException();
		}

		Map<Integer, URI> partitionMap = _sliceMappings.get(graphId);
		if (partitionMap == null) {
			return null;
		}

		return partitionMap.get(partitionId);
	}

	@Override
	public synchronized void putPartitionMapping(String graphId, int partitionId, URI uri) {
		if (graphId == null) {
			throw new IllegalArgumentException();
		}
		if (partitionId == Partition.INVALID_PARTITION) {
			throw new IllegalArgumentException();
		}
		if (uri == null || uri.isOpaque()) {
			throw new IllegalArgumentException();
		}

		Int2ObjectMap<URI> partitionMap = _sliceMappings.get(graphId);
		if (partitionMap == null) {
			partitionMap = new Int2ObjectOpenHashMap<>();
			_sliceMappings.put(graphId, partitionMap);
		}

		partitionMap.put(partitionId, uri.normalize());
	}

	@Override
	public synchronized void removePartitionMapping(String graphId, int partitionId) {
		if (graphId == null) {
			throw new IllegalArgumentException();
		}
		if (partitionId == Partition.INVALID_PARTITION) {
			throw new IllegalArgumentException();
		}

		Int2ObjectMap<URI> partitionMap = _sliceMappings.get(graphId);
		if (partitionMap == null) {
			return;
		}

		partitionMap.remove(partitionId);
		if (partitionMap.isEmpty()) {
			_sliceMappings.remove(graphId);
		}
	}

	@Override
	public synchronized boolean hasPartitionMapping(String graphId, int partitionId) {
		if (graphId == null) {
			throw new IllegalArgumentException();
		}
		if (partitionId == Partition.INVALID_PARTITION) {
			throw new IllegalArgumentException();
		}

		Int2ObjectMap<URI> partitionMap = _sliceMappings.get(graphId);
		if (partitionMap == null) {
			return false;
		}

		return partitionMap.containsKey(partitionId);
	}
}
