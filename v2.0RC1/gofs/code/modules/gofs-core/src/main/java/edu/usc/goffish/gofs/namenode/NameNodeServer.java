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

import java.io.*;
import java.net.*;

import org.glassfish.grizzly.http.server.*;

import com.sun.jersey.api.container.grizzly2.*;
import com.sun.jersey.api.core.*;
import com.sun.jersey.api.json.*;

/**
 * This class implements a REST server that maintains local name node state. Clients may use RemoteNameNode to
 * communicate with this server and read and write to its state via the INameNode interface.
 */
public class NameNodeServer {

	public static final String DATA_NODES = "datanodes";
	public static final String SERIALIZER = "serializer";
	public static final String DIRECTORY = "directory";
	public static final String AVAILABLE = "available";
	
	public static final String ADD_DATA_NODE_DATANODE_QP = "dataNode";
	public static final String GET_GRAPH_PARTITIONS_LOCATIONTOMATCH_QP = "locationToMatch";
	public static final String PUT_PARTITION_MAPPING_LOCATION_QP = "location";
	public static final String SET_SERIALIZER_SERIALIZERTYPE_QP = "serializerType";
	
	// singleton (sigh)
	private static final LocalNameNode NameNode = new LocalNameNode();

	private final HttpServer _server;

	public NameNodeServer(URI uri) throws IOException {
		ResourceConfig rc = new PackagesResourceConfig("edu.usc.goffish.gofs.namenode.resources");
		rc.getFeatures().put(JSONConfiguration.FEATURE_POJO_MAPPING, true);
		_server = GrizzlyServerFactory.createHttpServer(uri, rc);
	}

	public static LocalNameNode getNameNode() {
		return NameNode;
	}

	public void start() throws IOException {
		_server.start();
	}

	public void stop() {
		_server.stop();
	}
}
