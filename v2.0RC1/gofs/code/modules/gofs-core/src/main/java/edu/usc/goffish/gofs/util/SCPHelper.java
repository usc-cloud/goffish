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

package edu.usc.goffish.gofs.util;

import java.io.*;
import java.net.*;
import java.nio.file.*;
import java.util.*;

import edu.usc.goffish.gofs.util.ProcessHelper.ProcessInfo;

public final class SCPHelper {

	private static final Path DEFAULT_SCP_BINARY = Paths.get("scp");
	
	private SCPHelper() {
		throw new UnsupportedOperationException();
	}
	
	public static void SCP(Path file, URI remote) throws IOException {
		SCP(DEFAULT_SCP_BINARY, Collections.singletonList(file), remote);
	}
	
	public static ProcessInfo startSCP(Path file, URI remote) throws IOException {
		return startSCP(DEFAULT_SCP_BINARY, Collections.singletonList(file), remote);
	}
	
	public static void SCP(Path scpBinaryPath, Path file, URI remote) throws IOException {
		SCP(scpBinaryPath, Collections.singletonList(file), remote);
	}
	
	public static ProcessInfo startSCP(Path scpBinaryPath, Path file, URI remote) throws IOException {
		return startSCP(scpBinaryPath, Collections.singletonList(file), remote);
	}
	
	public static void SCP(List<Path> files, URI remote) throws IOException {
		SCP(DEFAULT_SCP_BINARY, null, files, remote);
	}
	
	public static ProcessInfo startSCP(List<Path> files, URI remote) throws IOException {
		return startSCP(DEFAULT_SCP_BINARY, null, files, remote);
	}
	
	public static void SCP(Path scpBinaryPath, List<Path> files, URI remote) throws IOException {
		SCP(scpBinaryPath, null, files, remote);
	}
	
	public static ProcessInfo startSCP(Path scpBinaryPath, List<Path> files, URI remote) throws IOException {
		return startSCP(scpBinaryPath, null, files, remote);
	}
	
	public static void SCP(Path scpBinaryPath, String[] scpExtraOptions, List<Path> files, URI remote) throws IOException {
		try {
			startSCP(scpBinaryPath, scpExtraOptions, files, remote).finish();
		} catch (InterruptedException e) {
			throw new IOException(e);
		}
	}
	
	public static ProcessInfo startSCP(Path scpBinaryPath, String[] scpExtraOptions, List<Path> files, URI remote) throws IOException {
		if (scpBinaryPath == null) {
			throw new IllegalArgumentException();
		}
		if (remote == null || remote.isOpaque()) {
			throw new IllegalArgumentException();
		}
		if (files == null || files.isEmpty()) {
			throw new IllegalArgumentException();
		}
		
		// retrieve SCP port if set
		String[] scpPortArgs = (remote.getPort() != -1) ? new String[]{"-P", Integer.toString(remote.getPort())} : null;
		
		// retrieve user info
		String remoteUserStr = remote.getUserInfo();
		remoteUserStr = (remoteUserStr != null) ? remoteUserStr + "@" : "";
		
		// retrieve remote host
		String remoteHostStr = remote.getHost();
		remoteHostStr = (remoteHostStr != null) ? remoteHostStr + ":" : "";
		
		// retrieve and escape remote path
		String remotePathStr = (remote.getPath() != null) ? escapePath(remote.getPath()) : "";
		
		if (remoteHostStr.isEmpty() && remotePathStr.isEmpty()) {
			// must have a destination scp argument
			throw new IllegalArgumentException();
		}
		
		// prepare command
		ProcessHelper.CommandBuilder command = new ProcessHelper.CommandBuilder(scpBinaryPath.toString());
		command.append("-B").append("-q").append("-r").append("-p");
		if (scpExtraOptions != null) {
			command.append(scpExtraOptions);
		}
		if (scpPortArgs != null) {
			command.append(scpPortArgs);
		}
		command.append(files).append(remoteUserStr + remoteHostStr + remotePathStr);
		
		// execute command
		System.out.println("executing: \"" + command + "\"");
		return ProcessHelper.startProcess(true, command.toArray());
	}
	
	private static String escapePath(String path) {
		return path.replace(" ", "\\ ");
	}
}
