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
import java.lang.ProcessBuilder.Redirect;
import java.nio.file.*;
import java.util.*;

public final class ProcessHelper {

	private ProcessHelper() {
		throw new UnsupportedOperationException();
	}

	public static ProcessInfo startProcess(String... command) throws IOException {
		return startProcess((File)null, false, command);
	}

	public static void runProcess(String... command) throws IOException, InterruptedException {
		runProcess((File)null, false, command);
	}

	public static ProcessInfo startProcess(boolean redirectOutput, String... command) throws IOException {
		return startProcess((File)null, redirectOutput, command);
	}

	public static void runProcess(boolean redirectOutput, String... command) throws IOException, InterruptedException {
		runProcess((File)null, redirectOutput, command);
	}

	public static ProcessInfo startProcess(Path workingDir, String... command) throws IOException {
		return startProcess(workingDir.toFile(), false, command);
	}

	public static void runProcess(Path workingDir, String... command) throws IOException, InterruptedException {
		runProcess(workingDir.toFile(), false, command);
	}

	public static ProcessInfo startProcess(Path workingDir, boolean redirectOutput, String... command) throws IOException {
		return startProcess(workingDir.toFile(), redirectOutput, command);
	}

	public static void runProcess(Path workingDir, boolean redirectOutput, String... command) throws IOException, InterruptedException {
		runProcess(workingDir.toFile(), redirectOutput, command);
	}

	public static ProcessInfo startProcess(File workingDir, String... command) throws IOException {
		return startProcess(workingDir, false, command);
	}

	public static void runProcess(File workingDir, String... command) throws IOException, InterruptedException {
		runProcess(workingDir, false, command);
	}

	public static ProcessInfo startProcess(File workingDir, boolean redirectOutput, String... command) throws IOException {
		if (command == null || command.length == 0) {
			throw new IllegalArgumentException();
		}

		ProcessBuilder b = new ProcessBuilder().command(command).directory(workingDir).redirectErrorStream(true);
		if (redirectOutput) {
			b.redirectOutput(Redirect.INHERIT);
		}

		return new ProcessInfo(b.start(), command);
	}

	public static void runProcess(File workingDir, boolean redirectOutput, String... command) throws IOException, InterruptedException {
		startProcess(workingDir, redirectOutput, command).finish();
	}

	private static String commandToString(String... command) {
		return commandToString(Arrays.asList(command));
	}
	
	private static String commandToString(List<String> command) {
		int length = 0;
		for (String arg : command) {
			length += arg.length() + 1;
		}

		StringBuilder b = new StringBuilder(length);
		for (String arg : command) {
			b.append(arg).append(" ");
		}
		if (b.length() > 0) {
			b.setLength(b.length() - 1);
		}

		return b.toString();
	}

	public static class CommandBuilder {

		private final List<String> _command;
		private String _commandString;
		private String[] _commandArray;

		public CommandBuilder(String command) {
			_command = new LinkedList<>();
			_command.add(command);
			_commandString = null;
			_commandArray = null;
		}

		public CommandBuilder append(String arg) {
			if (arg == null) {
				throw new IllegalArgumentException();
			}

			_command.add(arg);
			_commandString = null;
			_commandArray = null;
			return this;
		}
		
		public CommandBuilder append(Path arg) {
			if (arg == null) {
				throw new IllegalArgumentException();
			}

			return append(arg.toString());
		}

		public CommandBuilder append(String... args) {
			return append(Arrays.asList(args));
		}

		public CommandBuilder append(List<? extends Object> args) {
			if (args == null || args.size() == 0) {
				throw new IllegalArgumentException();
			}

			for (Object arg : args) {
				_command.add(arg.toString());
			}
			_commandString = null;
			_commandArray = null;
			return this;
		}

		public String[] toArray() {
			if (_commandArray == null) {
				_commandArray = _command.toArray(new String[_command.size()]);
			}

			return _commandArray;
		}

		@Override
		public String toString() {
			if (_commandString == null) {
				_commandString = commandToString(_command);
			}

			return _commandString;
		}
	}
	
	public static class ProcessInfo extends Process {

		private final Process _process;
		private final String[] _command;
		
		public ProcessInfo(Process process, String... command) {
			if (process == null) {
				throw new IllegalArgumentException();
			}
			if (command == null) {
				throw new IllegalArgumentException();
			}
			
			_process = process;
			_command = command;
		}
		
		public void finish() throws InterruptedException, IOException {
			try {
				int exit = _process.waitFor();
				if (exit != 0) {
					throw new IOException("process \"" + commandToString(_command) + "\" exited with value " + exit);
				}
			} finally {
				_process.destroy();
			}
		}
		
		public String[] getCommand() {
			return _command;
		}
		
		@Override
		public OutputStream getOutputStream() {
			return _process.getOutputStream();
		}

		@Override
		public InputStream getInputStream() {
			return _process.getInputStream();
		}

		@Override
		public InputStream getErrorStream() {
			return _process.getErrorStream();
		}

		@Override
		public int waitFor() throws InterruptedException {
			return _process.waitFor();
		}

		@Override
		public int exitValue() {
			return _process.exitValue();
		}

		@Override
		public void destroy() {
			_process.destroy();
		}

		@Override
		public int hashCode() {
			return _process.hashCode();
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			
			return _process.equals(((ProcessInfo)obj)._process);
		}

		@Override
		public String toString() {
			return _process.toString();
		}
	}
}
