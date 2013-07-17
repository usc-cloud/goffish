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
package edu.usc.goffish.gofs.namenode;

import java.io.*;
import java.net.*;
import java.util.*;

import edu.usc.goffish.gofs.*;
import edu.usc.goffish.gofs.partition.*;

/**
 * This class is an in-memory instance of INameNode. It is used as the basis for
 * many other INameNode implementations. This class is fully thread safe.
 */
public class LocalNameNode implements INameNode, Serializable {

	private static final long serialVersionUID = 1L;

	private final Map<String, Map<Integer, URI>> _sliceMappings;

	public LocalNameNode() {
		_sliceMappings = new HashMap<>();
	}

	/**
	 * Writes a human-readable version of the state of this name node to the
	 * given output stream. Meant to be used in conjunction with
	 * {@link #readReadable(InputStream)} for debugging purposes.
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
			for (Map.Entry<String, Map<Integer, URI>> entry1 : nameNode._sliceMappings.entrySet()) {
				for (Map.Entry<Integer, URI> entry2 : entry1.getValue().entrySet()) {
					output.write(entry1.getKey() + "->" + entry2.getKey() + "->" + entry2.getValue() + System.lineSeparator());
				}
			}
		}
		output.flush();
	}

	/**
	 * Reads a human-readable version of the state of this name node from the
	 * given input stream. Meant to be used in conjunction with
	 * {@link #writeReadable(LocalNameNode, OutputStream)} for debugging
	 * purposes.
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
	public void clearAndPutAll(LocalNameNode other) {
		if (other == null) {
			throw new IllegalArgumentException();
		}

		_sliceMappings.clear();
		for (Map.Entry<String, Map<Integer, URI>> entry : other._sliceMappings.entrySet()) {
			_sliceMappings.put(entry.getKey(), new HashMap<>(entry.getValue()));
		}
	}

	@Override
	public synchronized List<Integer> getPartitions(String graphId) {
		Map<Integer, URI> partitionMap = _sliceMappings.get(graphId);
		if (partitionMap == null) {
			return null;
		}

		return new ArrayList<Integer>(partitionMap.keySet());
	}

	@Override
	public synchronized List<Integer> getMatchingPartitions(String graphId, URI locationToMatch) {
		if (graphId == null) {
			throw new IllegalArgumentException();
		}
		if (locationToMatch == null || locationToMatch.isOpaque()) {
			throw new IllegalArgumentException();
		}

		locationToMatch = locationToMatch.normalize();

		final String scheme = locationToMatch.getScheme();
		final String userinfo = locationToMatch.getUserInfo();
		final String host = locationToMatch.getHost();
		final int port = locationToMatch.getPort();
		final URI path = URI.create(locationToMatch.getPath());
		final String query = locationToMatch.getQuery();
		final String fragment = locationToMatch.getFragment();

		LinkedList<Integer> matchingPartitions = new LinkedList<>();
		for (Map.Entry<Integer, URI> entry : _sliceMappings.get(graphId).entrySet()) {
			URI test = entry.getValue();

			// test scheme, userinfo, host, port, query, fragment
			if (scheme != null && !scheme.equalsIgnoreCase(test.getScheme())) {
				continue;
			}
			if (userinfo != null && !userinfo.equals(test.getUserInfo())) {
				continue;
			}
			if (host != null && !host.equals(test.getHost())) {
				continue;
			}
			if (port != -1 && port != test.getPort()) {
				continue;
			}
			if (query != null && !query.equals(test.getQuery())) {
				continue;
			}
			if (fragment != null && !fragment.equals(test.getFragment())) {
				continue;
			}

			// test path
			if (path != null) {
				if (test.getPath() == null) {
					continue;
				}

				URI testPath = URI.create(test.getPath());
				if (path.relativize(testPath) == testPath) {
					continue;
				}
			}

			// all tests passed, return mapping
			matchingPartitions.add(entry.getKey());
		}

		return matchingPartitions;
	}

	@Override
	public synchronized URI getPartitionMapping(String graphId, int partitionId) {
		if (graphId == null) {
			throw new IllegalArgumentException();
		}
		if (partitionId == BasePartition.INVALID_PARTITION) {
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
		if (partitionId == BasePartition.INVALID_PARTITION) {
			throw new IllegalArgumentException();
		}
		if (uri == null || uri.isOpaque()) {
			throw new IllegalArgumentException();
		}

		Map<Integer, URI> partitionMap = _sliceMappings.get(graphId);
		if (partitionMap == null) {
			partitionMap = new HashMap<>();
			_sliceMappings.put(graphId, partitionMap);
		}

		partitionMap.put(partitionId, uri.normalize());
	}

	@Override
	public synchronized void removePartitionMapping(String graphId, int partitionId) {
		if (graphId == null) {
			throw new IllegalArgumentException();
		}
		if (partitionId == BasePartition.INVALID_PARTITION) {
			throw new IllegalArgumentException();
		}

		Map<Integer, URI> partitionMap = _sliceMappings.get(graphId);
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
		if (partitionId == BasePartition.INVALID_PARTITION) {
			throw new IllegalArgumentException();
		}

		Map<Integer, URI> partitionMap = _sliceMappings.get(graphId);
		if (partitionMap == null) {
			return false;
		}

		return partitionMap.containsKey(partitionId);
	}
}
