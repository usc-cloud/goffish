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

import edu.usc.goffish.gofs.namenode.*;

public class GoFSNameNodeServer {

	private static Path savePath = null;

	public static void main(String[] args) throws IOException {
		if (args.length < REQUIRED_ARGS) {
			PrintUsageAndQuit(null);
		}

		if (args.length == 1 && args[0].equals("-help")) {
			PrintUsageAndQuit(null);
		}

		URI serverURI = null;

		int i = 0;

		// parse server uri
		try {
			serverURI = new URI(args[i]);
			i++;
		} catch (URISyntaxException e) {
			PrintUsageAndQuit("server uri - " + e.getMessage());
		}

		if (args.length > i) {
			try {
				savePath = Paths.get(args[i]);
				i++;
			} catch (InvalidPathException e) {
				PrintUsageAndQuit("save file path - " + e.getMessage());
			}
		}

		// finished parsing args
		if (i < args.length) {
			PrintUsageAndQuit("Unrecognized argument \"" + args[i] + "\"");
		}

		// load information from file
		if (savePath != null) {
			try (ObjectInputStream input = new ObjectInputStream(Files.newInputStream(savePath))) {
				LocalNameNode nameNode = LocalNameNode.readReadable(input);
				NameNodeServer.getNameNode().clearAndPutAll(nameNode);
				System.out.println("Loaded name node information from file.");
			} catch (IOException e) {
				System.out.println("Warning: Unable to load name node information from file - " + e.getMessage());
			}
		}

		// create server
		NameNodeServer server = new NameNodeServer(serverURI);

		// register shutdown hook
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				save();
			}
		});

		// run server
		try {
			server.start();

			BufferedReader input = new BufferedReader(new InputStreamReader(System.in));
			while (true) {
				String command = input.readLine().toLowerCase();

				if ("quit".equals(command)) {
					break;
				} else if ("display".equals(command)) {
					System.out.println("Name Node State:");
					LocalNameNode.writeReadable(NameNodeServer.getNameNode(), System.out);
				} else if ("save".equals(command)) {
					save();
				}
			}
		} finally {
			server.stop();
		}

		System.exit(0);
	}

	private static synchronized void save() {
		if (savePath == null) {
			return;
		}

		try (ObjectOutputStream output = new ObjectOutputStream(Files.newOutputStream(savePath))) {
			LocalNameNode.writeReadable(NameNodeServer.getNameNode(), output);
			System.out.println("Saved name node information to file.");
		} catch (Exception e) {
			System.out.println("Error: Unable to save name node information to file - " + e.getMessage());
		}
	}

	private static final int REQUIRED_ARGS = 1;

	private static void PrintUsageAndQuit(String error) {
		if (error != null) {
			System.out.println("Error: " + error);
		}

		System.out.println("Usage:");
		System.out.println("  GoFSNameNodeServer -help");
		System.out.println("      Displays this help message.");
		System.out.println("  GoFSNameNodeServer <uri> [<savefile>]");
		System.out.println();
		System.out.println("      This will start a name node server at the given URI. If a save file is");
		System.out.println("    specified, information will be loaded from the save file if the file exists.");
		System.out.println("    The name node state will be saved to the file on the save command and on");
		System.out.println("    shutdown. The server supports the following commands during operation:");
		System.out.println("      display : displays current state");
		System.out.println("      save : saves current state to given save file");
		System.out.println("      quit : shuts down the server, and saves to given save file");
		System.out.println();
		System.out.println("    Options:");
		System.out.println("      The <uri> specifies the uri to run this server on. Generally, the uri");
		System.out.println("    should be formatted something like 'http://localhost:9998'.");
		System.out.println("      The <savefile> is an optional argument which specifies the location of a");
		System.out.println("    file to load and save information to. This argument may be omitted.");

		System.exit(1);
	}
}
