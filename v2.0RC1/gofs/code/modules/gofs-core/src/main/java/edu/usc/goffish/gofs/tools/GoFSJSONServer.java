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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Paths;

import javax.ws.rs.core.UriBuilder;

import org.glassfish.grizzly.http.server.HttpServer;

import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.container.grizzly2.GrizzlyServerFactory;
import com.sun.jersey.api.core.PackagesResourceConfig;
import com.sun.jersey.api.core.ResourceConfig;
import com.sun.jersey.api.json.JSONConfiguration;

import edu.usc.goffish.gofs.IDataNode;
import edu.usc.goffish.gofs.IPartition;
import edu.usc.goffish.gofs.namenode.DataNode;

public class GoFSJSONServer {

	private final HttpServer server;

	private static IPartition gmlPartition;

	public GoFSJSONServer(String host, int port) throws IOException {
		ResourceConfig resourceConfig = new PackagesResourceConfig("edu.usc.goffish.gofs.json.resources");
		resourceConfig.getFeatures().put(JSONConfiguration.FEATURE_POJO_MAPPING, true);
		resourceConfig.getProperties().put(ClientConfig.FEATURE_DISABLE_XML_SECURITY, true);
		server = GrizzlyServerFactory.createHttpServer(UriBuilder.fromUri("").scheme("http").host(host).port(port).build(), resourceConfig);
	}

	public static IPartition getPartition(){
		return gmlPartition;
	}

	public void start() throws IOException {
		server.start();
	}

	public void stop() {
		server.stop();
	}

	public static void main(String[] args) throws IOException, URISyntaxException {
		if(args.length < 10){
			printUsageAndQuit(null);
		}

		String hostname = null, graphId = null;
		int port = -1, partitionId = -1;
		String datanodeArg;
		IDataNode datanode = null;
		for(int index = 0; index < args.length; index++){
			switch(args[index]){
			case "-host":
				hostname = args[++index];
				break;
			case "-port":
				try{
					port = Integer.parseInt(args[++index]);
					break;
				}catch(NumberFormatException e){
					printUsageAndQuit("port must be an integer.");
				}
			case "-graphid":
				graphId = args[++index];
				break;
			case "-partitionid":
				partitionId = Integer.parseInt(args[++index]);
				break;
			case "-datanode":
				datanodeArg = args[++index];
				String[] subArgs;
				if(datanodeArg.startsWith("path")){
					subArgs = datanodeArg.split("=");
					if(subArgs[0].equals("path")){
						datanode = DataNode.create(Paths.get(subArgs[1]));
					}else{
						printUsageAndQuit("data node should be specified with valid path.");
					}
				}else if(datanodeArg.startsWith("uri")){
					subArgs = datanodeArg.split("=");
					if(subArgs[0].equals("uri")){
						datanode = DataNode.create(URI.create(subArgs[1]));
					}else{
						printUsageAndQuit("data node should be specified with valid uri.");
					}
				}

				break;
			default:
				printUsageAndQuit(null);
			}
		}
		
		//Load the partition
		gmlPartition = datanode.loadLocalPartition(graphId, partitionId);
		GoFSJSONServer server = new GoFSJSONServer(hostname, port);

		try {
			//Start the server
			server.start();

			BufferedReader input = new BufferedReader(new InputStreamReader(System.in));
			while (true) {
				String command = input.readLine().toLowerCase();

				if ("quit".equals(command)) {
					break;
				}
			}
		} finally {
			server.stop();
		}
	}

	private static void printUsageAndQuit(String error){
		if(error != null){
			System.out.println(error);
		}

		System.out.println("usage:");
		System.out.println("GoFSJSONServer -host <hostname> -port <port> -graphid <graphid> -partitionid <partitionid> -datanode path=<datanodefilepath>|uri=<datanodefileuri>");
		System.exit(0);
	}
}
