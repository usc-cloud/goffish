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

package edu.usc.goffish.gofs.slice;

import java.io.*;

public class JavaSliceSerializer implements ISliceSerializer {

	private final int _bufferSize;

	public JavaSliceSerializer() {
		this(16384);
	}

	public JavaSliceSerializer(int bufferSize) {
		_bufferSize = bufferSize;
	}

	@Override
	public long serialize(ISlice slice, OutputStream stream) throws IOException {
		assert (slice != null);

		CountingBufferedOutputStream buffer = new CountingBufferedOutputStream(stream, _bufferSize);
		@SuppressWarnings("resource")
		ObjectOutputStream output = new ObjectOutputStream(buffer);

		output.writeObject(slice);
		output.flush();

		return buffer.total();
	}

	@Override
	public <T extends ISlice> T deserialize(InputStream stream, Class<T> deserializedClass) throws IOException {
		ObjectInputStream output = new ObjectInputStream(new BufferedInputStream(stream, _bufferSize));
		try {
			return deserializedClass.cast(output.readObject());
		} catch (ClassNotFoundException e) {
			throw new IOException(e);
		}
	}

	/**
	 * Drop in replacement for BufferedOutputStream that isn't synchronous and
	 * counts the number of bytes written.
	 */
	private final class CountingBufferedOutputStream extends FilterOutputStream {

		private byte buf[];
		private int count;
		private long total;

		public CountingBufferedOutputStream(OutputStream out, int size) {
			super(out);
			if (size <= 0) {
				throw new IllegalArgumentException("Buffer size <= 0");
			}
			buf = new byte[size];
		}

		private void flushBuffer() throws IOException {
			if (count > 0) {
				out.write(buf, 0, count);
				total += count;
				count = 0;
			}
		}

		public long total() {
			return total + count;
		}

		public void write(int b) throws IOException {
			if (count >= buf.length) {
				flushBuffer();
			}
			buf[count++] = (byte)b;
		}

		public void write(byte b[], int off, int len) throws IOException {
			if (len >= buf.length) {
				/*
				 * If the request length exceeds the size of the output buffer,
				 * flush the output buffer and then write the data directly. In
				 * this way buffered streams will cascade harmlessly.
				 */
				flushBuffer();
				out.write(b, off, len);
				total += len;
				return;
			}
			if (len > buf.length - count) {
				flushBuffer();
			}
			System.arraycopy(b, off, buf, count, len);
			count += len;
		}

		public void flush() throws IOException {
			flushBuffer();
			out.flush();
		}
	}
}
