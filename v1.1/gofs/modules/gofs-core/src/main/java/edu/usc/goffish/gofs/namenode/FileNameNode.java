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
import java.nio.channels.*;
import java.nio.file.*;
import java.util.*;

import edu.usc.goffish.gofs.*;

// TODO: more efficient serialization

/**
 * This name node implementation has no ongoing in memory state. Rather, on each
 * memory call the name node state is read from disk, and if modified, written
 * back out. This is meant for use over a distributed or network file system,
 * not for efficiency.
 */
public class FileNameNode implements INameNode {

	private final File _file;

	public FileNameNode(Path path) throws IOException {
		if (path == null) {
			throw new IllegalArgumentException();
		}

		_file = path.toFile();

		// create file if necessary and attempt to force early IOException
		try (RandomAccessFile file = new RandomAccessFile(_file, "rw")) {
		}
	}

	private static LocalNameNode loadFromFile(RandomAccessFile file) throws IOException {
		if (file.length() > 0) {
			byte[] bytes = new byte[(int)file.length()];
			file.seek(0);
			file.readFully(bytes);

			try (ByteArrayInputStream input = new ByteArrayInputStream(bytes)) {
				return LocalNameNode.readReadable(input);
			}
		} else {
			return new LocalNameNode();
		}
	}

	private static void writeToFile(RandomAccessFile file, LocalNameNode nameNode) throws IOException {
		try (ByteArrayOutputStream output = new ByteArrayOutputStream()) {
			LocalNameNode.writeReadable(nameNode, output);

			// unclear if this is the best way to overwrite a random access file
			file.setLength(0);
			file.write(output.toByteArray());
		}
	}

	@Override
	public List<Integer> getPartitions(String graphId) throws IOException {
		try (RandomAccessFile file = new RandomAccessFile(_file, "r")) {
			FileLock lock = file.getChannel().lock(0, Long.MAX_VALUE, true);
			try {
				LocalNameNode nameNode = loadFromFile(file);

				return nameNode.getPartitions(graphId);
			} finally {
				lock.release();
			}
		}
	}

	@Override
	public List<Integer> getMatchingPartitions(String graphId, URI locationToMatch) throws IOException {
		try (RandomAccessFile file = new RandomAccessFile(_file, "r")) {
			FileLock lock = file.getChannel().lock(0, Long.MAX_VALUE, true);
			try {
				LocalNameNode nameNode = loadFromFile(file);

				return nameNode.getMatchingPartitions(graphId, locationToMatch);
			} finally {
				lock.release();
			}
		}
	}

	@Override
	public URI getPartitionMapping(String graphId, int partitionId) throws IOException {
		try (RandomAccessFile file = new RandomAccessFile(_file, "r")) {
			FileLock lock = file.getChannel().lock(0, Long.MAX_VALUE, true);
			try {
				LocalNameNode nameNode = loadFromFile(file);

				return nameNode.getPartitionMapping(graphId, partitionId);
			} finally {
				lock.release();
			}
		}
	}

	@Override
	public boolean hasPartitionMapping(String graphId, int partitionId) throws IOException {
		try (RandomAccessFile file = new RandomAccessFile(_file, "r")) {
			FileLock lock = file.getChannel().lock(0, Long.MAX_VALUE, true);
			try {
				LocalNameNode nameNode = loadFromFile(file);

				return nameNode.hasPartitionMapping(graphId, partitionId);
			} finally {
				lock.release();
			}
		}
	}

	@Override
	public void putPartitionMapping(String graphId, int partitionId, URI location) throws IOException {
		try (RandomAccessFile file = new RandomAccessFile(_file, "rw")) {
			FileLock lock = file.getChannel().lock(0, Long.MAX_VALUE, false);
			try {
				LocalNameNode nameNode = loadFromFile(file);
				nameNode.putPartitionMapping(graphId, partitionId, location);
				writeToFile(file, nameNode);
			} finally {
				lock.release();
			}
		}
	}

	@Override
	public void removePartitionMapping(String graphId, int partitionId) throws IOException {
		try (RandomAccessFile file = new RandomAccessFile(_file, "rw")) {
			FileLock lock = file.getChannel().lock(0, Long.MAX_VALUE, false);
			try {
				LocalNameNode nameNode = loadFromFile(file);
				nameNode.removePartitionMapping(graphId, partitionId);
				writeToFile(file, nameNode);
			} finally {
				lock.release();
			}
		}
	}
}
