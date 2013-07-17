/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law
 * or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
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

	public static Process startProcess(String... command) throws IOException {
		return startProcess((File)null, false, command);
	}

	public static void runProcess(String... command) throws IOException, InterruptedException {
		runProcess((File)null, false, command);
	}

	public static Process startProcess(boolean redirectOutput, String... command) throws IOException {
		return startProcess((File)null, redirectOutput, command);
	}

	public static void runProcess(boolean redirectOutput, String... command) throws IOException, InterruptedException {
		runProcess((File)null, redirectOutput, command);
	}

	public static Process startProcess(Path workingDir, String... command) throws IOException {
		return startProcess(workingDir.toFile(), false, command);
	}

	public static void runProcess(Path workingDir, String... command) throws IOException, InterruptedException {
		runProcess(workingDir.toFile(), false, command);
	}

	public static Process startProcess(Path workingDir, boolean redirectOutput, String... command) throws IOException {
		return startProcess(workingDir.toFile(), redirectOutput, command);
	}

	public static void runProcess(Path workingDir, boolean redirectOutput, String... command) throws IOException, InterruptedException {
		runProcess(workingDir.toFile(), redirectOutput, command);
	}

	public static Process startProcess(File workingDir, String... command) throws IOException {
		return startProcess(workingDir, false, command);
	}

	public static void runProcess(File workingDir, String... command) throws IOException, InterruptedException {
		runProcess(workingDir, false, command);
	}

	public static Process startProcess(File workingDir, boolean redirectOutput, String... command) throws IOException {
		if (command == null || command.length == 0) {
			throw new IllegalArgumentException();
		}

		ProcessBuilder b = new ProcessBuilder().command(command).directory(workingDir).redirectErrorStream(true);
		if (redirectOutput) {
			b.redirectOutput(Redirect.INHERIT);
		}

		return b.start();
	}

	public static void runProcess(File workingDir, boolean redirectOutput, String... command) throws IOException, InterruptedException {
		Process p = startProcess(workingDir, redirectOutput, command);

		try {
			int exit = p.waitFor();
			if (exit != 0) {
				throw new IOException("process \"" + commandToString(command) + "\" exited with value " + exit);
			}
		} finally {
			p.destroy();
		}
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
}
