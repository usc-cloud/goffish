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
import java.nio.file.*;

import javax.ws.rs.core.*;

import org.glassfish.grizzly.http.server.*;

import com.sun.jersey.api.container.grizzly2.*;
import com.sun.jersey.api.core.*;
import com.sun.jersey.api.json.*;

/**
 * This class implements a REST server that maintains local name node state.
 * Clients may use RemoteNameNode to communicate with this server and read and
 * write to its state via the INameNode interface.
 */
public class NameNodeServer {

	// singleton (sigh)
	private static final LocalNameNode NameNode = new LocalNameNode();

	private final HttpServer _server;

	public NameNodeServer(String host, int port) throws IOException {
		ResourceConfig rc = new PackagesResourceConfig("edu.usc.goffish.gofs.namenode.resources");
		rc.getFeatures().put(JSONConfiguration.FEATURE_POJO_MAPPING, true);
		_server = GrizzlyServerFactory.createHttpServer(UriBuilder.fromUri("").scheme("http").host(host).port(port).build(), rc);
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

	public static void main(String[] args) throws IOException {
		Path savePath = null;
		String host = "localhost";
		int port = 9998;

		// parse arguments
		if (args.length > 1) {
			for (int i = 0; i < args.length; i++) {
				switch (args[i]) {
				case "-h":
					if (args.length < i + 2) {
						PrintUsageAndQuit(null);
					}

					String[] uri = args[i + 1].split(":");
					if (uri.length < 2) {
						PrintUsageAndQuit("server uri must include both host and port");
					}

					host = uri[0];
					try {
						port = Integer.parseInt(uri[1]);
					} catch (NumberFormatException e) {
						PrintUsageAndQuit("port must be an integer");
					}

					i += 1;
					break;
				case "-f":
					if (args.length < i + 2) {
						PrintUsageAndQuit(null);
					}

					try {
						savePath = Paths.get(args[i + 1]);
					} catch (InvalidPathException e) {
						PrintUsageAndQuit("save file must be a valid path");
					}

					i += 1;
					break;
				default:
					PrintUsageAndQuit(null);
					break;
				}
			}
		}

		// load information from file
		if (savePath != null) {
			try (ObjectInputStream input = new ObjectInputStream(Files.newInputStream(savePath))) {
				LocalNameNode nameNode = (LocalNameNode)input.readObject();
				NameNodeServer.getNameNode().clearAndPutAll(nameNode);
				System.out.println("Loaded name node information from file.");
			} catch (IOException e) {
				System.out.println("Warning: Unable to load name node information from file - " + e.getMessage());
			} catch (Exception e) {
				PrintUsageAndQuit(e.getMessage());
			}
		}

		// create server
		NameNodeServer server = new NameNodeServer(host, port);

		// run server
		try {
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

		// save information to file
		if (savePath != null) {
			try (ObjectOutputStream output = new ObjectOutputStream(Files.newOutputStream(savePath))) {
				output.writeObject(NameNodeServer.getNameNode());
				System.out.println("Saved name node information to file.");
			} catch (Exception e) {
				System.out.println("Warning: Unable to save name node information to file - " + e.getMessage());
			}
		}
	}

	private static void PrintUsageAndQuit(String error) {
		if (error != null) {
			System.out.println("Error: " + error);
		}

		System.out.println("Usage:");
		System.out.println("  NameNodeServer [-f <savefile>] [-h <host>:<port>]");
		System.out.println("  Default <host>:<port> is localhost:9998");
		System.exit(0);
	}
}
