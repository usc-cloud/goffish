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

package edu.usc.goffish.gofs.util.partitioning.metis;

import java.io.*;
import java.nio.file.*;

import org.apache.commons.io.*;

import edu.usc.goffish.gofs.formats.metis.*;
import edu.usc.goffish.gofs.graph.*;
import edu.usc.goffish.gofs.util.*;
import edu.usc.goffish.gofs.util.partitioning.*;

public class MetisPartitioner implements IPartitioner {

	private static final Path DefaultMetisBinary = Paths.get("gpmetis");
	private static final Path DefaultMetisInput = Paths.get("input.metis");

	private final Path _metisBinaryPath;
	private final String[] _extraMetisOptions;
	
	public MetisPartitioner() {
		this(DefaultMetisBinary, null);
	}

	public MetisPartitioner(Path metisBinaryPath, String[] extraMetisOptions) {
		if (metisBinaryPath == null) {
			throw new IllegalArgumentException();
		}

		_metisBinaryPath = metisBinaryPath.normalize();
		_extraMetisOptions = extraMetisOptions;
	}

	@Override
	public IPartitioning partition(IIdentifiableVertexGraph<? extends IIdentifiableVertex, ? extends IEdge> graph, int numPartitions) throws IOException {
		if (graph == null) {
			throw new IllegalArgumentException();
		}
		if (numPartitions < 1) {
			throw new IllegalArgumentException();
		}
		if (graph.isDirected()) {
			throw new IllegalArgumentException();
		}
		
		Path workingDir = Files.createTempDirectory("gofs_metis");
		try {
			// we assume the graph will always require renumbering
			System.out.print("writing metis input file (with renumbering)... ");
			long time = System.currentTimeMillis();
			
			// write metis input
			Path metisInputPath = workingDir.resolve(DefaultMetisInput);
			long[] renumbering = MetisGraph.writeAndRenumber(graph, Files.newOutputStream(metisInputPath));
			
			System.out.println("[" + (System.currentTimeMillis() - time) + "ms]");
			
			// perform partitioning
			Path metisOutputPath = partition(workingDir, metisInputPath, numPartitions);
			
			System.out.print("loading metis output... ");
			time = System.currentTimeMillis();
			
			// read metis output
			IPartitioning partitioning = MetisPartitioning.readAndRenumber(Files.newInputStream(metisOutputPath), renumbering);
			
			System.out.println("[" + (System.currentTimeMillis() - time) + "ms]");
			
			return partitioning;
		} finally {
			FileUtils.deleteQuietly(workingDir.toFile());
		}
	}
	
	public IPartitioning partition(Path metisInputPath, int numPartitions) throws IOException {
		if (metisInputPath == null) {
			throw new IllegalArgumentException();
		}
		if (numPartitions < 1) {
			throw new IllegalArgumentException();
		}
		
		Path workingDir = Files.createTempDirectory("working");
		try {
			// perform partitioning
			Path metisOutputPath = partition(workingDir, metisInputPath, numPartitions);
			
			System.out.print("loading metis output... ");
			long time = System.currentTimeMillis();
			
			// read metis output
			IPartitioning partitioning = MetisPartitioning.read(Files.newInputStream(metisOutputPath));
			
			System.out.println("[" + (System.currentTimeMillis() - time) + "ms]");
			
			return partitioning;
		} finally {
			FileUtils.deleteQuietly(workingDir.toFile());
		}
	}
	
	private Path partition(Path workingDir, Path metisInputPath, int numPartitions) throws IOException {

		// prepare metis command
		ProcessHelper.CommandBuilder command = new ProcessHelper.CommandBuilder(_metisBinaryPath.toString());
		if (_extraMetisOptions != null) {
			command.append(_extraMetisOptions);
		}
		command.append(metisInputPath).append(Integer.toString(numPartitions));

		// run metis
		try {
			System.out.println("executing: \"" + command + "\"");
			ProcessHelper.runProcess(workingDir.toFile(), true, command.toArray());
		} catch (InterruptedException e) {
			throw new IOException(e);
		}

		return workingDir.resolve(metisInputPath.getFileName() + ".part." + numPartitions);
	}
}
