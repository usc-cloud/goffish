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
import java.util.*;

import edu.usc.goffish.gofs.formats.gml.*;
import edu.usc.goffish.gofs.formats.metis.*;

public final class PartitionGML {

	private PartitionGML() {
		throw new UnsupportedOperationException();
	}

	public static void main(String[] args) throws IOException {
		if (args.length < NumRequiredArgs) {
			PrintUsageAndQuit(null);
		}

		Path partitionFile = null;
		Path outputDir = null;
		Path templateFile = null;
		List<Path> instanceFiles = new LinkedList<>();

		try {
			partitionFile = Paths.get(args[0]);
		} catch (InvalidPathException e) {
			PrintUsageAndQuit("partition file must be a valid path - " + e.getMessage());
		}

		try {
			outputDir = Paths.get(args[1]);
		} catch (InvalidPathException e) {
			PrintUsageAndQuit("output directory must be a valid path - " + e.getMessage());
		}

		try {
			Path gmlInputFile = Paths.get(args[2]);
			try (BufferedReader input = new BufferedReader(new InputStreamReader(Files.newInputStream(gmlInputFile)))) {
				String line = input.readLine();
				templateFile = Paths.get(line);

				line = input.readLine();
				while (line != null) {
					instanceFiles.add(Paths.get(line));
					line = input.readLine();
				}
			} catch (InvalidPathException e) {
				PrintUsageAndQuit("gml input file must contain valid paths - " + e.getMessage());
			}
		} catch (InvalidPathException e) {
			PrintUsageAndQuit("gml input file must be a valid path - " + e.getMessage());
		}

		// create output directory
		Files.createDirectory(outputDir);

		// create and use partitioner
		GMLPartitioner partitioner = new GMLPartitioner(MetisPartitioning.read(Files.newInputStream(partitionFile)));

		System.out.println("partitioning template...");
		partitioner.partitionTemplate(Files.newInputStream(templateFile), outputDir, "partition_", "_template.gml");

		int j = 1;
		for (Path instanceFile : instanceFiles) {
			System.out.println("partitioning instance " + j + "...");
			partitioner.partitionInstance(Files.newInputStream(instanceFile), outputDir, "partition_", "_instance_" + j + ".gml");
			j++;
		}

		System.out.println("finished");
	}

	private static final int NumRequiredArgs = 3;

	private static void PrintUsageAndQuit(String error) {
		if (error != null) {
			System.out.println("Error: " + error);
		}

		System.out.println("Usage:");
		System.out.println("  PartitionGML <metispartitioningfile> <outputdir> <gmlinputfile>");
		System.out.println("    <gmlinputfile> is a text file with one gml file location per line. the template file should be");
		System.out.println("    on the first line, instance files on every subsequent line.");
		System.exit(0);
	}
}
