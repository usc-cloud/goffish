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

package edu.usc.goffish.gofs.util;

import java.io.*;
import java.net.*;
import java.nio.file.*;
import java.util.*;

public final class SCPHelper {

	private static final Path DEFAULT_SCP_BINARY = Paths.get("scp");
	
	private SCPHelper() {
		throw new UnsupportedOperationException();
	}
	
	public static void SCP(Path file, URI remote) throws IOException {
		SCP(DEFAULT_SCP_BINARY, Collections.singletonList(file), remote);
	}
	
	public static void SCP(Path scpBinaryPath, Path file, URI remote) throws IOException {
		SCP(scpBinaryPath, Collections.singletonList(file), remote);
	}
	
	public static void SCP(List<Path> files, URI remote) throws IOException {
		SCP(DEFAULT_SCP_BINARY, null, files, remote);
	}
	
	public static void SCP(Path scpBinaryPath, List<Path> files, URI remote) throws IOException {
		SCP(scpBinaryPath, null, files, remote);
	}
	
	public static void SCP(Path scpBinaryPath, String[] scpExtraOptions, List<Path> files, URI remote) throws IOException {
		if (scpBinaryPath == null) {
			throw new IllegalArgumentException();
		}
		if (remote == null) {
			throw new IllegalArgumentException();
		}
		if (files == null || files.isEmpty()) {
			throw new IllegalArgumentException();
		}
		
		// parse scp arguments
		String[] scpPortArgs = (remote.getPort() != -1) ? new String[]{"-P", Integer.toString(remote.getPort())} : null;
		
		String remoteUserStr = remote.getUserInfo();
		remoteUserStr = (remoteUserStr != null) ? remoteUserStr + "@" : "";
		
		String remoteHostStr = remote.getHost();
		if (remoteHostStr == null) {
			// uri must contain a hostname
			throw new IllegalArgumentException();
		}
		
		String remotePathStr = remote.getPath();
		if (remotePathStr == null) {
			// uri must contain a path
			throw new IllegalArgumentException();
		}
		remotePathStr = remotePathStr.replace(" ", "\\ ");
		
		// prepare command
		ProcessHelper.CommandBuilder command = new ProcessHelper.CommandBuilder(scpBinaryPath.toString());
		command.append("-B").append("-q");
		if (scpExtraOptions != null) {
			command.append(scpExtraOptions);
		}
		if (scpPortArgs != null) {
			command.append(scpPortArgs);
		}
		command.append(files).append(remoteUserStr + remoteHostStr + ":" + remotePathStr);
		
		// execute command
		System.out.println("executing: \"" + command + "\"");
		try {
			ProcessHelper.runProcess(false, command.toArray());
		} catch (InterruptedException e) {
			throw new IOException(e);
		}
	}
}
