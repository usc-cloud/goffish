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

import edu.usc.goffish.gofs.util.ProcessHelper.ProcessInfo;

public final class SSHHelper {

	private static final Path DEFAULT_SSH_BINARY = Paths.get("ssh");
	
	private SSHHelper() {
		throw new UnsupportedOperationException();
	}
	
	public static void SSH(URI remote, String... remoteCommands) throws IOException {
		SSH(DEFAULT_SSH_BINARY, null, remote, remoteCommands);
	}
	
	public static ProcessInfo startSSH(URI remote, String... remoteCommands) throws IOException {
		return startSSH(DEFAULT_SSH_BINARY, null, remote, remoteCommands);
	}
	
	public static void SSH(Path sshBinaryPath, URI remote, String... remoteCommands) throws IOException {
		SSH(sshBinaryPath, null, remoteCommands);
	}
	
	public static ProcessInfo startSSH(Path sshBinaryPath, URI remote, String... remoteCommands) throws IOException {
		return startSSH(sshBinaryPath, null, remoteCommands);
	}
	
	public static void SSH(Path sshBinaryPath, String[] sshExtraOptions, URI remote, String... remoteCommands) throws IOException {
		try {
			startSSH(sshBinaryPath, sshExtraOptions, remote, remoteCommands).finish();
		} catch (InterruptedException e) {
			throw new IOException(e);
		}
	}
	
	public static ProcessInfo startSSH(Path sshBinaryPath, String[] sshExtraOptions, URI remote, String... remoteCommands) throws IOException {
		if (sshBinaryPath == null) {
			throw new IllegalArgumentException();
		}
		if (remote == null || remote.isOpaque()) {
			throw new IllegalArgumentException();
		}
		if (remoteCommands == null) {
			throw new IllegalArgumentException();
		}
		
		// retrieve ssh port if set
		String[] sshPortArgs = (remote.getPort() != -1) ? new String[]{"-p", Integer.toString(remote.getPort())} : null;
		
		// retrieve user info
		String remoteUserStr = remote.getUserInfo();
		remoteUserStr = (remoteUserStr != null) ? remoteUserStr + "@" : "";
		
		// retrieve remote host
		String remoteHostStr = remote.getHost();
		if (remoteHostStr == null) {
			// uri must contain a hostname
			throw new IllegalArgumentException();
		}
		
		String commandString = "";
		
		// retrieve and escape remote path
		String remotePathStr = remote.getPath();
		if (remotePathStr != null && !remotePathStr.isEmpty()) {
			commandString += escapeCommand("cd " + remotePathStr) + ";";
		}
		
		// escape remote command
		for (String remoteCommand : remoteCommands) {
			commandString += escapeCommand(remoteCommand) + ";";
		}
		
		// prepare command
		ProcessHelper.CommandBuilder command = new ProcessHelper.CommandBuilder(sshBinaryPath.toString());
		if (sshExtraOptions != null) {
			command.append(sshExtraOptions);
		}
		if (sshPortArgs != null) {
			command.append(sshPortArgs);
		}
		command.append(remoteUserStr + remoteHostStr).append(commandString);
		
		// execute command
		System.out.println("executing: \"" + command + "\"");
		return ProcessHelper.startProcess(true, command.toArray());
	}
	
	private static String escapeCommand(String command) {
		return command.replace(";", "\\;");
	}
}
