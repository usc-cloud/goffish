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

public class TemporaryFileStorageManager implements IStorageManager {

	private final Map<UUID, Path> _temporaryFiles;

	public TemporaryFileStorageManager() {
		_temporaryFiles = new HashMap<>();
	}

	@Override
	public OutputStream getWriteStream(UUID sliceUUID) throws IOException {
		Path file = Files.createTempFile(null, ".slc");
		file.toFile().deleteOnExit();
		OutputStream stream = Files.newOutputStream(file);
		_temporaryFiles.put(sliceUUID, file);
		return stream;
	}

	@Override
	public InputStream getReadStream(UUID sliceUUID) throws IOException {
		Path file = _temporaryFiles.get(sliceUUID);
		if (file == null) {
			throw new IOException();
		}

		return Files.newInputStream(file);
	}

	@Override
	public boolean hasReadStream(UUID sliceUUID) {
		return _temporaryFiles.containsKey(sliceUUID);
	}

	@Override
	public boolean remove(UUID sliceUUID) throws IOException {
		Path path = _temporaryFiles.remove(sliceUUID);
		if (path != null) {
			Files.deleteIfExists(path);
		}

		return true;
	}
}
