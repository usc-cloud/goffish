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
package edu.usc.goffish.gofs.tools;

import java.io.*;
import java.nio.file.*;

import edu.usc.goffish.gofs.*;
import edu.usc.goffish.gofs.formats.gml.*;
import edu.usc.goffish.gofs.formats.metis.*;

public final class GenerateMetisGraph {

	private static enum Mode {
		NONE, GML
	};

	private GenerateMetisGraph() {
		throw new UnsupportedOperationException();
	}

	private static ISubgraphTemplate loadGML(Path gmlTemplatePath) throws IOException {
		GMLGraph gmlGraph = GMLGraph.readForceDirectedness(Files.newInputStream(gmlTemplatePath), false, false);
		if (!gmlGraph.getRemoteVertexMappings().isEmpty()) {
			// gml template may not contain remote vertices if it is to be
			// converted to metis format
			throw new IllegalArgumentException();
		}
		if (!gmlGraph.getTemplate().containsVertex(1)) {
			// gml template vertices must start numbering at 1 and move upwards
			// continuously if the template is to be converted to metis format
			throw new IllegalArgumentException();
		}
		
		return gmlGraph.getTemplate();
	}

	public static void main(String[] args) throws IOException {
		if (args.length < NumRequiredArgs) {
			PrintUsageAndQuit(null);
		}

		Mode mode = Mode.NONE;
		Path gmlTemplatePath = null;
		Path outputMetisPath = null;

		// parse optional args

		int i;
		for (i = 0; i < args.length - NumRequiredArgs; i++) {
			switch (args[i]) {
			case "-gml":
				mode = Mode.GML;
				try {
					gmlTemplatePath = Paths.get(args[i + 1]);
				} catch (InvalidPathException e) {
					PrintUsageAndQuit("gml template file must be a valid path - " + e.getMessage());
				}

				i++;
				break;
			}
		}

		// parse required args

		if (args.length - i < NumRequiredArgs) {
			PrintUsageAndQuit(null);
		}

		try {
			outputMetisPath = Paths.get(args[i]);
		} catch (InvalidPathException e) {
			PrintUsageAndQuit("output metis file must be a valid path - " + e.getMessage());
		}

		System.out.println("loading graph...");
		
		// load graph
		ISubgraphTemplate graph = null;
		switch (mode) {
		case GML:
			graph = loadGML(gmlTemplatePath);
			break;
		default:
			PrintUsageAndQuit("no input mode chosen");
			break;
		}

		System.out.println("converting to METIS format and outputting...");
		
		int numVerticesWritten = MetisGraph.write(graph, Files.newOutputStream(outputMetisPath));
		if (numVerticesWritten != graph.numVertices()) {
			System.out.println("WARNING: Only " + numVerticesWritten + "/" + graph.numVertices() + " vertices were written, the graph is likely not numbered consecutively");
		}
		
		System.out.println("finished");
	}

	private static final int NumRequiredArgs = 1;

	private static void PrintUsageAndQuit(String error) {
		if (error != null) {
			System.out.println("Error: " + error);
		}

		System.out.println("Usage:");
		System.out.println("  GenerateMetisGraph -gml <gmltemplatefile> <outputmetisfile>");
		System.exit(0);
	}
}
