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

package edu.usc.goffish.gofs.tools;

import java.io.*;
import java.net.*;
import java.nio.file.*;
import java.util.*;

import org.apache.commons.configuration.*;
import org.apache.commons.io.*;

import edu.usc.goffish.gofs.namenode.*;
import edu.usc.goffish.gofs.slice.*;
import edu.usc.goffish.gofs.util.*;

public class GoFSFormat {
	
	private static final String DATANODE_DIR_NAME = "gofs/"; // MUST end with a directory separator so this is treated as a folder
	
	private static final Path DEFAULT_CONFIG = Paths.get("../conf/gofs.config");

	public static final String GOFS_DATANODES_KEY = "gofs.datanode";
	public static final String GOFS_SERIALIZER_KEY = "gofs.serializer";
	public static final String GOFS_NAMENODE_TYPE_KEY = "gofs.namenode.type";
	public static final String GOFS_NAMENODE_LOCATION_KEY = "gofs.namenode.location";
	
	private GoFSFormat() {
		throw new UnsupportedOperationException();
	}
	
	public static void main(String[] args) throws IOException {
		if (args.length < REQUIRED_ARGS) {
			PrintUsageAndQuit(null);
		}
		
		if (args.length == 1 && args[0].equals("-help")) {
			PrintUsageAndQuit(null);
		}
		
		Path executableDirectory;
		try {
			executableDirectory = Paths.get(GoFSFormat.class.getProtectionDomain().getCodeSource().getLocation().toURI()).getParent();
		} catch (URISyntaxException e) {
			throw new RuntimeException("Unexpected error retrieving executable location", e);
		}
		Path configPath = executableDirectory.resolve(DEFAULT_CONFIG).normalize();
		
		boolean copyBinaries = false;
		
		// parse optional arguments
		int i = 0;
		OptArgLoop: for (i = 0; i < args.length - REQUIRED_ARGS; i++) {
			switch (args[i]) {
			case "-config":
				i++;

				try {
					configPath = Paths.get(args[i]);
				} catch (InvalidPathException e) {
					PrintUsageAndQuit("Config file - " + e.getMessage());
				}

				break;
			case "-copyBinaries":
				copyBinaries = true;
				break;
			default:
				break OptArgLoop;
			}
		}

		if (args.length - i < REQUIRED_ARGS) {
			PrintUsageAndQuit(null);
		}
		
		// finished parsing args
		if (i < args.length) {
			PrintUsageAndQuit("Unrecognized argument \"" + args[i] + "\"");
		}
		
		// parse config
		
		System.out.println("Parsing config...");

		PropertiesConfiguration config = new PropertiesConfiguration();
		config.setDelimiterParsingDisabled(true);
		try {
			config.load(Files.newInputStream(configPath));
		} catch (ConfigurationException e) {
			throw new IOException(e);
		}

		// retrieve data nodes
		ArrayList<URI> dataNodes;
		{
			String[] dataNodesArray = config.getStringArray(GOFS_DATANODES_KEY);
			if (dataNodesArray.length == 0) {
				throw new ConversionException("Config must contain key " + GOFS_DATANODES_KEY);
			}
			
			dataNodes = new ArrayList<>(dataNodesArray.length);
			
			if (dataNodesArray.length == 0) {
				throw new ConversionException("Config key " + GOFS_DATANODES_KEY + " has invalid format - must define at least one data node");
			}
			
			try {
				for (String node : dataNodesArray) {
					URI dataNodeURI = new URI(node);
					
					if (!"file".equalsIgnoreCase(dataNodeURI.getScheme())) {
						throw new ConversionException("config key " + GOFS_DATANODES_KEY + " value \"" + dataNodeURI + "\" has invalid format - data node urls must have 'file' scheme");
					} else if (dataNodeURI.getPath() == null || dataNodeURI.getPath().isEmpty()) {
						throw new ConversionException("config key " + GOFS_DATANODES_KEY + " value \"" + dataNodeURI + "\" has invalid format - data node urls must have an absolute path specified");
					}

					// ensure uri ends with a slash, so we know it is a directory
					if (!dataNodeURI.getPath().endsWith("/")) {
						dataNodeURI = dataNodeURI.resolve(dataNodeURI.getPath() + "/");
					}
					
					dataNodes.add(dataNodeURI);
				}
			} catch (URISyntaxException e) {
				throw new ConversionException("Config key " + GOFS_DATANODES_KEY + " has invalid format - " + e.getMessage());
			}
		}

		// validate serializer type
		Class<? extends ISliceSerializer> serializerType;
		{
			String serializerTypeName = config.getString(GOFS_SERIALIZER_KEY);
			if (serializerTypeName == null) {
				throw new ConversionException("Config must contain key " + GOFS_SERIALIZER_KEY);
			}
			
			try {
				serializerType = SliceSerializerProvider.loadSliceSerializerType(serializerTypeName);
			} catch (ReflectiveOperationException e) {
				throw new ConversionException("Config key " + GOFS_SERIALIZER_KEY + " has invalid format - " + e.getMessage());
			}
		}
		
		// retrieve name node
		IInternalNameNode nameNode;
		try {
			nameNode = NameNodeProvider.loadNameNodeFromConfig(config, GOFS_NAMENODE_TYPE_KEY, GOFS_NAMENODE_LOCATION_KEY);
		} catch (ReflectiveOperationException e) {
			throw new RuntimeException("Unable to load name node", e);
		}
		
		System.out.println("Contacting name node...");
		
		// validate name node
		if (!nameNode.isAvailable()) {
			throw new IOException("Name node at " + nameNode.getURI() + " is not available");
		}
		
		System.out.println("Contacting data nodes...");
		
		// validate data nodes
		for (URI dataNode : dataNodes) {
			// only attempt ssh if host exists
			if (dataNode.getHost() != null) {
				try {
					SSHHelper.SSH(dataNode, "true");
				} catch (IOException e) {
					throw new IOException("Data node at " + dataNode + " is not available", e);
				}
			}
		}
		
		// create temporary directory
		Path workingDir = Files.createTempDirectory("gofs_format");
		try {
			// create deploy directory
			Path deployDirectory = Files.createDirectory(workingDir.resolve(DATANODE_DIR_NAME));
			
			// create empty slice directory
			Files.createDirectory(deployDirectory.resolve(DataNode.DATANODE_SLICE_DIR));
			
			// copy binaries
			if (copyBinaries) {
				System.out.println("Copying binaries...");
				FileUtils.copyDirectory(executableDirectory.toFile(), deployDirectory.resolve(executableDirectory.getFileName()).toFile());
			}
			
			// write config file
			Path dataNodeConfigFile = deployDirectory.resolve(DataNode.DATANODE_CONFIG);
			try {
				// create config for every data node and scp deploy folder into place
				for (URI dataNodeParent : dataNodes) {
					URI dataNode = dataNodeParent.resolve(DATANODE_DIR_NAME);
					
					PropertiesConfiguration datanode_config = new PropertiesConfiguration();
					datanode_config.setDelimiterParsingDisabled(true);
					datanode_config.setProperty(DataNode.DATANODE_INSTALLED_KEY, true);
					datanode_config.setProperty(DataNode.DATANODE_NAMENODE_TYPE_KEY, config.getString(GOFS_NAMENODE_TYPE_KEY));
					datanode_config.setProperty(DataNode.DATANODE_NAMENODE_LOCATION_KEY, config.getString(GOFS_NAMENODE_LOCATION_KEY));
					datanode_config.setProperty(DataNode.DATANODE_LOCALHOSTURI_KEY, dataNode.toString());
					
					try {
						datanode_config.save(Files.newOutputStream(dataNodeConfigFile));
					} catch (ConfigurationException e) {
						throw new IOException(e);
					}
					
					System.out.println("Formatting data node " + dataNode.toString() + "...");
					
					// scp everything into place on the data node
					SCPHelper.SCP(deployDirectory, dataNodeParent);

					// update name node
					nameNode.addDataNode(dataNode);
				}
				
				// update name node
				nameNode.setSerializer(serializerType);
			} catch (Exception e) {
				System.out.println("ERROR: data node formatting interrupted - name node and data nodes are in an inconsistent state and require clean up");
				throw e;
			}
			
			System.out.println("GoFS format complete");
			
		} finally {
			FileUtils.deleteQuietly(workingDir.toFile());
		}
	}

	private static final int REQUIRED_ARGS = 0;
	
	private static void PrintUsageAndQuit(String error) {
		if (error != null) {
			System.out.println("Error: " + error);
		}

		System.out.println("Usage:");
		System.out.println("  GoFSInstantiate -help");
		System.out.println("      Displays this help message.");
		System.out.println("  GoFSInstantiate [args]");
		System.out.println("  Args: [-config <configfilepath>]");
		System.out.println("        [-copyBinaries]");
		System.out.println();
		System.out.println("      This will format the GoFS file system on a set of data nodes and add the");
		System.out.println("    data nodes to the name node. By default it will attempt to read the config");
		System.out.println("    file from the /conf directory of the installation for input. The user");
		System.out.println("    should edit this config file prior to running this command. The name node");
		System.out.println("    should also be available prior to running this command.");
		System.out.println();
		System.out.println("    Options:");
		System.out.println("      The -config flag specifies a config file to use instead of the default.");
		System.out.println("      The -copyBinaries flag specifies that GoFS binaries should be copied to");
		System.out.println("     each data node location as well.");
		
		System.exit(1);
	}
}
