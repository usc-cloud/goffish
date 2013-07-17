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

package edu.usc.goffish.gofs.tools.deploy;

import java.io.*;
import java.nio.file.*;
import java.util.*;

import edu.usc.goffish.gofs.*;
import edu.usc.goffish.gofs.formats.gml.*;
import edu.usc.goffish.gofs.partition.*;
import edu.usc.goffish.gofs.partition.gml.*;
import edu.usc.goffish.gofs.util.*;
import edu.usc.goffish.gofs.util.partitioning.*;

public class GMLPartitionBuilder implements IPartitionBuilder, Iterable<ISerializablePartition>, Closeable {

	private final Path _templatePath;
	private final List<Path> _instancePaths;

	private final Map<Integer, Path> _partitionedTemplates;
	private final Map<Integer, List<Path>> _partitionedInstances;

	private Path _workingDirPath;

	public GMLPartitionBuilder(Path templatePath, List<Path> instancePaths) {
		if (templatePath == null) {
			throw new IllegalArgumentException();
		}
		if (instancePaths == null) {
			throw new IllegalArgumentException();
		}

		_templatePath = templatePath;
		_instancePaths = new LinkedList<>(instancePaths);

		_partitionedTemplates = new HashMap<>();
		_partitionedInstances = new HashMap<>();
		
		_workingDirPath = null;
	}
	
	public GMLPartitionBuilder(int partitionId, Path templatePath, List<Path> instancePaths) {
		if (partitionId == BasePartition.INVALID_PARTITION) {
			throw new IllegalArgumentException();
		}
		if (templatePath == null) {
			throw new IllegalArgumentException();
		}
		if (instancePaths == null) {
			throw new IllegalArgumentException();
		}

		_templatePath = null;
		_instancePaths = null;

		_partitionedTemplates = new HashMap<>(1, 1f);
		_partitionedTemplates.put(partitionId, templatePath);
		_partitionedInstances = new HashMap<>(1, 1f);
		_partitionedInstances.put(partitionId, new LinkedList<>(instancePaths));
		
		_workingDirPath = null;
	}

	@Override
	public void close() {
		if (_workingDirPath != null) {
			FileHelper.delete(_workingDirPath.toFile());
		}
	}

	@Override
	public void buildPartitions(IPartitioning partitioning) throws IOException {
		if (!_partitionedTemplates.isEmpty() || !_partitionedInstances.isEmpty()) {
			// builder created with partitions already built, cannot call buildPartitions again
			throw new IllegalStateException();
		}
		
		if (_workingDirPath == null) {
			_workingDirPath = Files.createTempDirectory("gofs_gmlpartition");
		}
		
		// partition gml files
		GMLPartitioner partitioner = new GMLPartitioner(partitioning);

		System.out.print("partitioning template... ");
		long time = System.currentTimeMillis();
		partitioner.partitionTemplate(Files.newInputStream(_templatePath), _workingDirPath, "partition_", "_template.gml");
		System.out.println("[" + (System.currentTimeMillis() - time) + "ms]");

		int instanceId = 1;
		for (Path instancePath : _instancePaths) {
			System.out.print("partitioning instance " + instanceId + "... ");
			time = System.currentTimeMillis();
			partitioner.partitionInstance(Files.newInputStream(instancePath), _workingDirPath, "partition_", "_instance" + instanceId + ".gml");
			System.out.println("[" + (System.currentTimeMillis() - time) + "ms]");
			
			instanceId++;
		}

		_partitionedTemplates.clear();
		_partitionedInstances.clear();
		
		// collate partitions
		System.out.println("collating partitions...");
		for (int partitionId : partitioner.getPartitions()) {
			_partitionedTemplates.put(partitionId, _workingDirPath.resolve("partition_" + partitionId + "_template.gml"));
			LinkedList<Path> instances = new LinkedList<>();
			for (instanceId = 1; instanceId <= _instancePaths.size(); instanceId++) {
				instances.add(_workingDirPath.resolve("partition_" + partitionId + "_instance" + instanceId + ".gml"));
			}
			_partitionedInstances.put(partitionId, instances);
		}
	}

	public Map<Integer, Path> getPartitionedTemplatePaths() {
		return Collections.unmodifiableMap(_partitionedTemplates);
	}
	
	public Map<Integer, List<Path>> getPartitionedInstancesPaths() {
		return Collections.unmodifiableMap(_partitionedInstances);
	}
	
	@Override
	public Iterable<ISerializablePartition> getPartitions() throws IOException {
		return this;
	}

	@Override
	public Iterator<ISerializablePartition> iterator() {
		return new PartitionIterator();
	}

	private class PartitionIterator extends AbstractWrapperIterator<ISerializablePartition> {

		private final Iterator<Integer> _partitionIterator;

		public PartitionIterator() {
			_partitionIterator = _partitionedTemplates.keySet().iterator();
		}

		@Override
		protected ISerializablePartition advanceToNext() {
			if (!_partitionIterator.hasNext()) {
				return null;
			}

			int partitionId = _partitionIterator.next();
			Path templateFile = _partitionedTemplates.get(partitionId);
			List<Path> instanceFiles = _partitionedInstances.get(partitionId);

			try {
				return GMLPartition.parseGML(partitionId, Files.newInputStream(templateFile), new GMLFileIterable(instanceFiles));
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}
	}
}
