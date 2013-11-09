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
import java.nio.file.*;
import java.util.*;

import org.apache.commons.configuration.*;

import edu.usc.goffish.gofs.*;
import edu.usc.goffish.gofs.slice.*;
import edu.usc.goffish.gofs.util.*;

public class DataNode implements IDataNode {
	
	public static final Path DATANODE_CONFIG = Paths.get("gofs.config");
	public static final Path DATANODE_SLICE_DIR = Paths.get("slices/");
	
	public static final String DATANODE_INSTALLED_KEY = "datanode.installed";
	public static final String DATANODE_LOCALHOSTURI_KEY = "datanode.localhosturi";
	public static final String DATANODE_NAMENODE_TYPE_KEY = "datanode.namenode.type";
	public static final String DATANODE_NAMENODE_LOCATION_KEY = "datanode.namenode.location";
	
	private final Path _dataNodePath;
	private final Path _dataNodeSliceDirPath;
	private final PropertiesConfiguration _config;
	
	private final URI _localhostURI;
	private final IInternalNameNode _nameNode;
	private final ISliceSerializer _sliceSerializer;
	
	private DataNode(Path dataNodePath) throws IOException {
		_dataNodePath = dataNodePath.normalize().toAbsolutePath();
		_dataNodeSliceDirPath = _dataNodePath.resolve(DATANODE_SLICE_DIR);
		
		_config = new PropertiesConfiguration();
		_config.setDelimiterParsingDisabled(true);
		try {
			_config.load(Files.newInputStream(_dataNodePath.resolve(DATANODE_CONFIG)));
		} catch (ConfigurationException e) {
			throw new IOException(e);
		}

		// ensure this is valid data node
		boolean installed = _config.getBoolean(DATANODE_INSTALLED_KEY);
		if (!installed) {
			throw new InvalidPropertiesFormatException("data node config must contain key " + DATANODE_INSTALLED_KEY);
		}

		// retrieve localhost uri
		{
			String localhostString = _config.getString(DATANODE_LOCALHOSTURI_KEY);
			if (localhostString == null) {
				throw new InvalidPropertiesFormatException("data node config must contain key " + DATANODE_LOCALHOSTURI_KEY);
			}
			
			try {
				_localhostURI = new URI(localhostString);
			} catch (URISyntaxException e) {
				throw new InvalidPropertiesFormatException("data node config key " + DATANODE_LOCALHOSTURI_KEY + " has invalid format - " + e.getMessage());
			}
		}
		
		// retrieve name node
		try {
			_nameNode = NameNodeProvider.loadNameNodeFromConfig(_config, DATANODE_NAMENODE_TYPE_KEY, DATANODE_NAMENODE_LOCATION_KEY);
		} catch (ReflectiveOperationException e) {
			throw new RuntimeException("Unable to load name node", e);
		}
		
		// retrieve slice manager
		_sliceSerializer = _nameNode.getSerializer();
	}
	
	public static IDataNode create(Path dataNodePath) throws IOException {
		return new DataNode(dataNodePath);
	}
	
	public static IDataNode create(URI dataNodeURI) throws IOException {
		return new DataNode(convertLocalURIToPath(dataNodeURI));
	}

	@Override
	public Path getPath() {
		return _dataNodePath;
	}

	@Override
	public Configuration getConfig() {
		return _config;
	}

	@Override
	public INameNode getNameNode() {
		return _nameNode;
	}

	@Override
	public IntCollection getLocalPartitions(String graphId) throws IOException {
		return _nameNode.getPartitionDirectory().getMatchingPartitions(graphId, _localhostURI);
	}

	@Override
	public IPartition loadLocalPartition(String graphId, int partitionId) throws IOException {
		URI partitionURI = _nameNode.getPartitionDirectory().getPartitionMapping(graphId, partitionId);
		URI relativization = _localhostURI.relativize(partitionURI);
		
		if (relativization == partitionURI) {
			// given partition is not in this data node
			throw new IllegalArgumentException();
		}
		
		Path slicePath = _dataNodeSliceDirPath.resolve(relativization.getPath());
		
		UUID partitionUUID;
		try {
			partitionUUID = UUID.fromString(relativization.getFragment());
		} catch (NullPointerException | IllegalArgumentException e) {
			// bad fragment returned by partition directory
			throw new IllegalStateException(e);
		}
		
		return SliceManager.create(partitionUUID, _sliceSerializer, new FileStorageManager(slicePath)).readPartition();
	}
	
	private static Path convertLocalURIToPath(URI localURI) throws IOException {
		if (localURI == null || localURI.isOpaque() || !localURI.isAbsolute() || !"file".equalsIgnoreCase(localURI.getScheme())) {
			throw new IllegalArgumentException();
		}
		if (!URIHelper.isLocalURI(localURI)) {
			throw new IOException("uri host " + localURI.getHost() + " is not local");
		}
		
    	String path = localURI.getPath();
    	if (path == null) {
    		path = "/";
    	}
        return Paths.get(URI.create("file://" + path));
	}
}
