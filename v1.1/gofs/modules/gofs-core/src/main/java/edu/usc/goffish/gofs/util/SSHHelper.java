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

public final class SSHHelper {

	private static final Path DEFAULT_SSH_BINARY = Paths.get("ssh");
	
	private SSHHelper() {
		throw new UnsupportedOperationException();
	}
	
	public static Process startSSH(URI remote, String remoteCommand) throws IOException {
		return startSSH(DEFAULT_SSH_BINARY, null, remote, remoteCommand);
	}
	
	public static Process startSSH(Path sshBinaryPath, URI remote, String remoteCommand) throws IOException {
		return startSSH(sshBinaryPath, null, remoteCommand);
	}
	
	public static Process startSSH(Path sshBinaryPath, String[] sshExtraOptions, URI remote, String remoteCommand) throws IOException {
		if (sshBinaryPath == null) {
			throw new IllegalArgumentException();
		}
		if (remoteCommand == null) {
			throw new IllegalArgumentException();
		}
		
		// parse ssh arguments
		String[] sshPortArgs = (remote.getPort() != -1) ? new String[]{"-p", Integer.toString(remote.getPort())} : null;
		
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
		
		// escape remote command
		remoteCommand = "\"" + remoteCommand.replace("\"", "\\\"") + "\"";
		
		// prepare command
		ProcessHelper.CommandBuilder command = new ProcessHelper.CommandBuilder(sshBinaryPath.toString());
		if (sshExtraOptions != null) {
			command.append(sshExtraOptions);
		}
		if (sshPortArgs != null) {
			command.append(sshPortArgs);
		}
		command.append(remoteUserStr + remoteHostStr).append(remoteCommand);
		
		// execute command
		System.out.println("executing: \"" + command + "\"");
		return ProcessHelper.startProcess(false, command.toArray());
	}
}
