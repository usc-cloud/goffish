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

package edu.usc.goffish.gofs.slice;

import java.io.*;

import edu.usc.goffish.gofs.util.*;

public final class JavaSliceSerializer implements ISliceSerializer {

	private final int _bufferSize;

	public JavaSliceSerializer() {
		this(16384);
	}

	public JavaSliceSerializer(int bufferSize) {
		_bufferSize = bufferSize;
	}

	@Override
	public OutputStream prepareStream(OutputStream stream) throws IOException {
		return new CountingBufferedOutputStream(stream, _bufferSize);
	}

	@Override
	public InputStream prepareStream(InputStream stream) throws IOException {
		return new BufferedInputStream(stream, _bufferSize);
	}

	@Override
	public long serialize(ISlice slice, OutputStream stream) throws IOException {
		assert (slice != null);
		
		if (!(stream instanceof CountingBufferedOutputStream)) {
			// stream must be prepared with prepareStream
			throw new IOException();
		}
		
		CountingBufferedOutputStream buffer = (CountingBufferedOutputStream)stream;
		
		ObjectOutputStream output = new ObjectOutputStream(buffer);
		output.writeObject(slice);
		output.flush();

		return buffer.total();
	}

	@Override
	public <T extends ISlice> T deserialize(InputStream stream, Class<T> deserializedClass) throws IOException {
		if (!(stream instanceof BufferedInputStream)) {
			// stream must be prepared with prepareStream
			throw new IOException();
		}
		
		ObjectInputStream input = new ObjectInputStream(stream);
		try {
			return deserializedClass.cast(input.readObject());
		} catch (ClassNotFoundException e) {
			throw new IOException(e);
		}
	}
}
