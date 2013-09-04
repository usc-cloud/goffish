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

package edu.usc.goffish.gofs.formats.gml;

import java.io.*;
import java.util.*;

import org.apache.commons.lang.*;

final class GMLWriter implements Closeable, Flushable {

	private static final int DEFAULT_BUFFER_SIZE = 16384;

	private static final String TABULATOR = "  ";
	private static final String KEY_VALUE_SEPARATOR = " ";
	private static final String NEWLINE = System.lineSeparator();
	private static final String OPEN_LIST = "[";
	private static final String CLOSE_LIST = "]";

	private final BufferedWriter _output;
	private final boolean _writeIndentation;

	private int _indentation;

	public GMLWriter(OutputStream output) {
		this(output, false, DEFAULT_BUFFER_SIZE);
	}

	public GMLWriter(OutputStream output, int bufferSize) {
		this(output, false, bufferSize);
	}

	public GMLWriter(OutputStream output, boolean writeIndentation, int bufferSize) {
		_output = new BufferedWriter(new OutputStreamWriter(output, GMLParser.GML_CHARSET), bufferSize);
		_writeIndentation = writeIndentation;
		_indentation = 0;
	}

	public static String classTypeToGMLType(Class<? extends Object> type) {
		if (type == String.class) {
			return GMLParser.STRING_TYPE;
		} else if (type == Integer.class) {
			return GMLParser.INTEGER_TYPE;
		} else if (type == Long.class) {
			return GMLParser.LONG_TYPE;
		} else if (type == Float.class) {
			return GMLParser.FLOAT_TYPE;
		} else if (type == Double.class) {
			return GMLParser.DOUBLE_TYPE;
		} else if (type == Boolean.class) {
			return GMLParser.BOOLEAN_TYPE;
		} else if (type == List.class) {
			return GMLParser.LIST_TYPE;
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
		} else if (value instanceof List) {
			@SuppressWarnings("unchecked")
			List<Object> list = (List<Object>)value;
			List<KeyValuePair> children = new ArrayList<>(list.size());
			for (Object v : list) {
				children.add(KeyValuePair.createKVP("value", v));
			}
			return children;
		}

		return value;
	}

	public void write(KeyValuePair kvp) throws IOException {
		if (kvp == null) {
			throw new IllegalArgumentException();
		}

		write(_indentation, kvp);
	}

	public void write(String key, Iterable<KeyValuePair> value) throws IOException {
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
		if (kvp instanceof ListKeyValuePair) {
			writeKVPStart(_indentation, kvp.Key());
			writeKVPValue(_indentation, kvp.ValueAsList());
			writeKVPEnd(_indentation);
		} else {
			if (_writeIndentation) {
				for (int i = 0; i < indentation; i++) {
					_output.write(TABULATOR);
				}
			}

			kvp.write(_output);
			_output.write(System.lineSeparator());
		}
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
		_output.write(StringEscapeUtils.escapeHtml(value));
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
