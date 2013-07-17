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

package edu.usc.goffish.gofs.formats.gml;

import java.io.*;
import java.util.*;

import org.apache.commons.lang3.*;

class GMLWriter implements Closeable, Flushable {

	private static final String TABULATOR = "  ";
	private static final String KEY_VALUE_SEPARATOR = " ";
	private static final String NEWLINE = System.lineSeparator();
	private static final String OPEN_LIST = "[";
	private static final String CLOSE_LIST = "]";

	private final BufferedWriter _output;
	private final boolean _writeIndentation;
	
	private int _indentation;

	public GMLWriter(OutputStream output) {
		this(output, false);
	}
	
	public GMLWriter(OutputStream output, boolean writeIndentation) {
		_output = new BufferedWriter(new OutputStreamWriter(output));
		_writeIndentation = writeIndentation;
		_indentation = 0;
	}

	public static String classTypeToGMLType(Class<? extends Object> type) {
		if (type == String.class) {
			return GMLParser.StringType;
		} else if (type == Integer.class) {
			return GMLParser.IntegerType;
		} else if (type == Long.class) {
			return GMLParser.LongType;
		} else if (type == Float.class) {
			return GMLParser.FloatType;
		} else if (type == Double.class) {
			return GMLParser.DoubleType;
		} else if (type == Boolean.class) {
			return GMLParser.BooleanType;
		}

		// type was not recognized
		throw new ClassCastException();
	}

	public static Object classValueToGMLValue(Object value) {
		if (value instanceof Integer) {
			return new Long(((Integer)value).longValue());
		} else if (value instanceof Float) {
			return new Double(((Float)value).doubleValue());
		} else if (value instanceof Boolean) {
			return new Long(((Boolean)value).booleanValue() ? 1L : 0L);
		}

		return value;
	}

	public void write(KeyValuePair kvp) throws IOException {
		if (kvp == null) {
			throw new IllegalArgumentException();
		}

		write(_indentation, kvp);
	}

	public void write(String key, List<KeyValuePair> value) throws IOException {
		if (key == null) {
			throw new IllegalArgumentException();
		}
		if (value == null) {
			throw new IllegalArgumentException();
		}

		writeKVPStart(_indentation, key);
		writeKVPValue(_indentation, value);
		writeKVPEnd(_indentation);
	}

	public void write(String key, String value) throws IOException {
		if (key == null) {
			throw new IllegalArgumentException();
		}
		if (value == null) {
			throw new IllegalArgumentException();
		}

		writeKVPStart(_indentation, key);
		writeKVPValue(_indentation, value);
		writeKVPEnd(_indentation);
	}

	public void write(String key, long value) throws IOException {
		if (key == null) {
			throw new IllegalArgumentException();
		}

		writeKVPStart(_indentation, key);
		writeKVPValue(_indentation, value);
		writeKVPEnd(_indentation);
	}

	public void write(String key, double value) throws IOException {
		if (key == null) {
			throw new IllegalArgumentException();
		}

		writeKVPStart(_indentation, key);
		writeKVPValue(_indentation, value);
		writeKVPEnd(_indentation);
	}

	public void writeListOpen(String key) throws IOException {
		if (key == null) {
			throw new IllegalArgumentException();
		}

		if (_writeIndentation) {
			for (int i = 0; i < _indentation; i++) {
				_output.write(TABULATOR);
			}
		}

		_output.write(key);
		_output.write(KEY_VALUE_SEPARATOR);
		_output.write(OPEN_LIST);
		_output.write(NEWLINE);

		_indentation++;
	}

	public void writeListClose() throws IOException {
		if (_indentation <= 0) {
			throw new IllegalStateException();
		}

		_indentation--;

		if (_writeIndentation) {
			for (int i = 0; i < _indentation; i++) {
				_output.write(TABULATOR);
			}
		}

		_output.write(CLOSE_LIST);
		_output.write(NEWLINE);
	}

	protected void write(int indentation, KeyValuePair kvp) throws IOException {
		writeKVPStart(indentation, kvp.Key());

		if (kvp instanceof CharArrayKeyValuePair) {
			writeKVPValue(indentation, ((CharArrayKeyValuePair)kvp).ValueAsCharArray());
		} else if (kvp.getValueType() == List.class) {
			writeKVPValue(indentation, kvp.ValueAsList());
		} else if (kvp.getValueType() == String.class) {
			writeKVPValue(indentation, kvp.ValueAsString());
		} else if (kvp.getValueType() == Long.class) {
			writeKVPValue(indentation, kvp.ValueAsLong());
		} else if (kvp.getValueType() == Double.class) {
			writeKVPValue(indentation, kvp.ValueAsDouble());
		} else {
			// unknown value type
			throw new ClassCastException();
		}

		writeKVPEnd(indentation);
	}

	private void writeKVPStart(int indentation, String key) throws IOException {
		if (_writeIndentation) {
			for (int i = 0; i < indentation; i++) {
				_output.write(TABULATOR);
			}
		}

		_output.write(key);
		_output.write(KEY_VALUE_SEPARATOR);
	}

	private void writeKVPValue(int indentation, Iterable<KeyValuePair> value) throws IOException {
		_output.write(OPEN_LIST);
		_output.write(NEWLINE);

		for (KeyValuePair childKvp : value) {
			write(indentation + 1, childKvp);
		}

		if (_writeIndentation) {
			for (int i = 0; i < indentation; i++) {
				_output.write(TABULATOR);
			}
		}

		_output.write(CLOSE_LIST);
	}

	private void writeKVPValue(int indentation, String value) throws IOException {
		_output.write("\"");
		_output.write(StringEscapeUtils.escapeHtml4(value));
		_output.write("\"");
	}
	
	private void writeKVPValue(int indentation, char[] value) throws IOException {
		_output.write("\"");
		_output.write(value);
		_output.write("\"");
	}

	private void writeKVPValue(int indentation, long value) throws IOException {
		_output.write(Long.toString(value));
	}

	private void writeKVPValue(int indentation, double value) throws IOException {
		_output.write(Double.toString(value));
	}

	private void writeKVPEnd(int indentation) throws IOException {
		_output.write(NEWLINE);
	}

	@Override
	public void flush() throws IOException {
		_output.flush();
	}

	@Override
	public void close() throws IOException {
		if (_indentation != 0) {
			throw new GMLFormatException("unbalanced open/close of gml lists (" + _indentation + " remain open)");
		}

		_output.close();
	}
}
