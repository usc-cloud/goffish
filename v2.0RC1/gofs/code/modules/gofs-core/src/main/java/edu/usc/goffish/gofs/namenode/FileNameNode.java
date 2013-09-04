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
import java.nio.channels.*;
import java.nio.file.*;
import java.util.*;

import edu.usc.goffish.gofs.*;
import edu.usc.goffish.gofs.slice.*;

/**
 * This name node implementation has no ongoing in memory state. Rather, on each
 * memory call the name node state is read from disk, and if modified, written
 * back out. This is meant for use over a distributed or network file system,
 * or for testing purposes.
 */
public class FileNameNode implements IInternalNameNode, IPartitionDirectory {

	private final File _file;
	private final URI _location;

	public FileNameNode(URI location) throws IOException {
		if (location == null) {
			throw new IllegalArgumentException();
		}

		_file = Paths.get(location).toFile();
		_location = location;
		
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
	public URI getURI() {
		return _location;
	}

	@Override
	public boolean isAvailable() {
		try (RandomAccessFile file = new RandomAccessFile(_file, "r")) {
			FileLock lock = file.getChannel().lock(0, Long.MAX_VALUE, true);
			try {
				loadFromFile(file);
			} finally {
				lock.release();
			}
			return true;
		} catch (Exception e) {}
		
		return false; 
	}

	@Override
	public Set<URI> getDataNodes() throws IOException {
		try (RandomAccessFile file = new RandomAccessFile(_file, "r")) {
			FileLock lock = file.getChannel().lock(0, Long.MAX_VALUE, true);
			try {
				LocalNameNode nameNode = loadFromFile(file);

				return nameNode.getDataNodes();
			} finally {
				lock.release();
			}
		}
	}

	@Override
	public void addDataNode(URI dataNode) throws IOException {
		try (RandomAccessFile file = new RandomAccessFile(_file, "rw")) {
			FileLock lock = file.getChannel().lock(0, Long.MAX_VALUE, false);
			try {
				LocalNameNode nameNode = loadFromFile(file);
				nameNode.addDataNode(dataNode);
				writeToFile(file, nameNode);
			} finally {
				lock.release();
			}
		}
	}

	@Override
	public void clearDataNodes() throws IOException {
		try (RandomAccessFile file = new RandomAccessFile(_file, "rw")) {
			FileLock lock = file.getChannel().lock(0, Long.MAX_VALUE, false);
			try {
				LocalNameNode nameNode = loadFromFile(file);
				nameNode.clearDataNodes();
				writeToFile(file, nameNode);
			} finally {
				lock.release();
			}
		}
	}

	@Override
	public ISliceSerializer getSerializer() throws IOException {
		try (RandomAccessFile file = new RandomAccessFile(_file, "r")) {
			FileLock lock = file.getChannel().lock(0, Long.MAX_VALUE, true);
			try {
				LocalNameNode nameNode = loadFromFile(file);

				return nameNode.getSerializer();
			} finally {
				lock.release();
			}
		}
	}

	@Override
	public void setSerializer(Class<? extends ISliceSerializer> sliceSerializer) throws IOException {
		try (RandomAccessFile file = new RandomAccessFile(_file, "rw")) {
			FileLock lock = file.getChannel().lock(0, Long.MAX_VALUE, false);
			try {
				LocalNameNode nameNode = loadFromFile(file);
				nameNode.setSerializer(sliceSerializer);
				writeToFile(file, nameNode);
			} finally {
				lock.release();
			}
		}
	}
	
	@Override
	public IPartitionDirectory getPartitionDirectory() {
		return this;
	}

	@Override
	public Collection<String> getGraphs() throws IOException {
		try (RandomAccessFile file = new RandomAccessFile(_file, "r")) {
			FileLock lock = file.getChannel().lock(0, Long.MAX_VALUE, true);
			try {
				LocalNameNode nameNode = loadFromFile(file);

				return nameNode.getGraphs();
			} finally {
				lock.release();
			}
		}
	}
	
	@Override
	public IntCollection getPartitions(String graphId) throws IOException {
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
	public IntCollection getMatchingPartitions(String graphId, URI locationToMatch) throws IOException {
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
