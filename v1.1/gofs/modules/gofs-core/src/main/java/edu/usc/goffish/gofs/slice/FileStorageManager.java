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
package edu.usc.goffish.gofs.slice;

import java.io.*;
import java.nio.file.*;
import java.util.*;

public class FileStorageManager implements IStorageManager {

	private final Path _sliceDir;

	public FileStorageManager(Path directory) {
		_sliceDir = directory.toAbsolutePath().normalize();
	}

	@Override
	public OutputStream getWriteStream(UUID sliceUUID) throws IOException {
		Files.createDirectories(_sliceDir);
		return Files.newOutputStream(_sliceDir.resolve(translateUUIDToFile(sliceUUID)));
	}

	@Override
	public InputStream getReadStream(UUID sliceUUID) throws IOException {
		return Files.newInputStream(_sliceDir.resolve(translateUUIDToFile(sliceUUID)));
	}

	@Override
	public boolean hasReadStream(UUID sliceUUID) {
		return Files.exists(_sliceDir.resolve(translateUUIDToFile(sliceUUID)));
	}

	@Override
	public boolean remove(UUID sliceUUID) throws IOException {
		Path path  = _sliceDir.resolve(translateUUIDToFile(sliceUUID));
		path.toFile().deleteOnExit();
		return Files.deleteIfExists(path);
	}

	private static String translateUUIDToFile(UUID uuid) {
		return uuid.toString() + ".slc";
	}
}
