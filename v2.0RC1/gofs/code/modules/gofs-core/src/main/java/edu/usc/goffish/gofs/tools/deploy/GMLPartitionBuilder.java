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

package edu.usc.goffish.gofs.tools.deploy;

import java.io.*;
import java.nio.file.*;
import java.util.*;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.HierarchicalConfiguration;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.commons.io.*;

import edu.usc.goffish.gofs.formats.gml.*;
import edu.usc.goffish.gofs.partition.*;
import edu.usc.goffish.gofs.partition.components.*;
import edu.usc.goffish.gofs.util.*;
import edu.usc.goffish.gofs.util.partitioning.*;

public class GMLPartitionBuilder implements IPartitionBuilder, Iterable<ISerializablePartition>, Closeable {

	private final IGraphComponentizer _partitionComponentizer;

	private final Path _templatePath;
	private final List<Path> _instancePaths;

	private final Map<Integer, Path> _partitionedTemplates;
	private final Map<Integer, List<Path>> _partitionedInstances;
	
	private boolean _isIntermediateGML;
	private Path _workingDirPath;

	public GMLPartitionBuilder(IGraphComponentizer graphComponentizer, Path templatePath, List<Path> instancePaths) {
		if (graphComponentizer == null) {
			throw new IllegalArgumentException();
		}
		if (templatePath == null) {
			throw new IllegalArgumentException();
		}
		if (instancePaths == null) {
			throw new IllegalArgumentException();
		}

		_partitionComponentizer = graphComponentizer;

		_templatePath = templatePath;
		_instancePaths = new LinkedList<>(instancePaths);

		_partitionedTemplates = new HashMap<>();
		_partitionedInstances = new HashMap<>();

		_workingDirPath = null;
	}

	public GMLPartitionBuilder(Path partitionedFilePath, IGraphComponentizer graphComponentizer, Path templatePath, List<Path> instancePaths) {
		this(graphComponentizer, templatePath, instancePaths);

		_workingDirPath = partitionedFilePath;
		_isIntermediateGML = true;
	}

	public GMLPartitionBuilder(int partitionId, IGraphComponentizer graphComponentizer, Path templatePath, List<Path> instancePaths) {
		if (partitionId == Partition.INVALID_PARTITION) {
			throw new IllegalArgumentException();
		}
		if (graphComponentizer == null) {
			throw new IllegalArgumentException();
		}
		if (templatePath == null) {
			throw new IllegalArgumentException();
		}
		if (instancePaths == null) {
			throw new IllegalArgumentException();
		}

		_partitionComponentizer = graphComponentizer;

		_templatePath = null;
		_instancePaths = null;

		_partitionedTemplates = new HashMap<>(1, 1f);
		_partitionedTemplates.put(partitionId, templatePath);
		_partitionedInstances = new HashMap<>(1, 1f);
		_partitionedInstances.put(partitionId, new LinkedList<>(instancePaths));

		_workingDirPath = null;
	}

	public GMLPartitionBuilder(IGraphComponentizer graphComponentizer) {
		_partitionComponentizer = graphComponentizer;

		_workingDirPath = null;
		_templatePath = null;
		_instancePaths = null;

		_partitionedTemplates = new HashMap<>();
		_partitionedInstances = new HashMap<>();
	}

	@Override
	public void close() {
		if (_workingDirPath != null && !_isIntermediateGML) {
			FileUtils.deleteQuietly(_workingDirPath.toFile());
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
		}else{
			if(!Files.exists(_workingDirPath)){
				Files.createDirectories(_workingDirPath);
			}
		}

		// partition gml files
		GMLPartitioner partitioner = new GMLPartitioner(partitioning);

		System.out.print("partitioning template... ");
		long time = System.currentTimeMillis();
		long writeTime = partitioner.partitionTemplate(Files.newInputStream(_templatePath), _workingDirPath, "partition_", "_template.gml");
		System.out.println("[" + writeTime + "ms writing] [" + (System.currentTimeMillis() - time) + "ms]");

		int instanceId = 1;
		for (Path instancePath : _instancePaths) {
			System.out.print("partitioning instance " + instanceId + "... ");
			time = System.currentTimeMillis();
			writeTime = partitioner.partitionInstance(Files.newInputStream(instancePath), _workingDirPath, "partition_", "_instance" + instanceId + ".gml");
			System.out.println("[" + writeTime + "ms writing] [" + (System.currentTimeMillis() - time) + "ms]");

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
				return GMLPartition.parseGML(partitionId, _partitionComponentizer, Files.newInputStream(templateFile), new GMLFileIterable(instanceFiles));
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
	}

	public class XMLConfigurationBuilder {
		private String _intermediateGMLInputFile;
		public XMLConfigurationBuilder(String intermediateGMLInputFile){
			this._intermediateGMLInputFile = intermediateGMLInputFile;
		}
		
		public void saveIntermediateGMLFile() throws IOException{ 
			XMLConfiguration configuration = getConfiguration();
			configuration.setRootElementName("gml");
			List<Path> instancePathCollection;
			for(int partitionId : _partitionedTemplates.keySet()){
				configuration.addProperty("partition(-1)[@id]", partitionId);
				configuration.addProperty("partition.template", _partitionedTemplates.get(partitionId).toFile().getAbsolutePath());

				instancePathCollection = _partitionedInstances.get(partitionId);
				for(Path instancePath : instancePathCollection){
					configuration.addProperty("partition.instances.instance(-1)", instancePath.toFile().getAbsolutePath());
				}
			}

			try {
				File partitionedGMLFileDir = _workingDirPath.resolve(_intermediateGMLInputFile).toFile();
				configuration.save(partitionedGMLFileDir);
				System.out.println("saving the partitioned gml information to " + partitionedGMLFileDir.getAbsolutePath());
			} catch (ConfigurationException e) {
				throw new RuntimeException(e);
			}
		}

		public void readIntermediateGMLFile() throws IOException{
			XMLConfiguration configuration = getConfiguration();
			try {
				configuration.load(new File(_intermediateGMLInputFile));
				System.out.println("reading the partitioned gml information from " + _intermediateGMLInputFile);
			} catch (ConfigurationException e) {
				throw new RuntimeException(e);
			}

			List<HierarchicalConfiguration> subConfigurationCollection;
			int partitionId;
			List<Object> instanceObjectCollection;
			List<Path> instancePaths;
			subConfigurationCollection = configuration.configurationsAt("partition");
			for(HierarchicalConfiguration subConfig : subConfigurationCollection){
				partitionId = subConfig.getInt("[@id]");
				_partitionedTemplates.put(partitionId, Paths.get(subConfig.getString("template")));

				instanceObjectCollection = subConfig.getList("instances.instance");
				instancePaths = new LinkedList<>();
				for(Object instanceObj : instanceObjectCollection){
					instancePaths.add(Paths.get(instanceObj.toString()));
				}

				_partitionedInstances.put(partitionId, instancePaths);
			}
		}

		private XMLConfiguration getConfiguration() {
			XMLConfiguration configuration = new XMLConfiguration();
			configuration.setDelimiterParsingDisabled(true);
			return configuration;
		}
	}
}
