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

package edu.usc.goffish.gofs.formats.gml;

import java.io.*;
import java.nio.*;
import java.nio.channels.*;
import java.nio.channels.FileChannel.MapMode;
import java.nio.charset.*;
import java.util.*;

final class GMLParser {

	// TODO: casing
	public static final String GraphKey = "graph";
	public static final String GraphDirectedKey = "directed";
	public static final String GraphVertexPropertiesKey = "vertex_properties";
	public static final String GraphEdgePropertiesKey = "edge_properties";
	public static final String GraphPropertyIsStatic = "is_static";
	public static final String GraphPropertyType = "type";
	public static final String GraphInstanceIdKey = "id";
	public static final String GraphInstanceTimestampStartKey = "timestamp_start";
	public static final String GraphInstanceTimestampEndKey = "timestamp_end";
	public static final String VertexKey = "node";
	public static final String VertexIdKey = "id";
	public static final String VertexRemoteKey = "remote";
	public static final String EdgeKey = "edge";
	public static final String EdgeIdKey = "id";
	public static final String EdgeSourceKey = "source";
	public static final String EdgeSinkKey = "target";

	public static final String StringType = "string";
	public static final String IntegerType = "integer";
	public static final String LongType = "long";
	public static final String FloatType = "float";
	public static final String DoubleType = "double";
	public static final String BooleanType = "boolean";

	public static final List<String> VertexKeys = Collections.unmodifiableList(Arrays.asList(new String[]{ VertexIdKey, VertexRemoteKey }));
	public static final List<String> EdgeKeys = Collections.unmodifiableList(Arrays.asList(new String[]{ EdgeIdKey, EdgeSourceKey, EdgeSinkKey }));
	public static final List<String> GraphKeys = Collections.unmodifiableList(Arrays.asList(new String[]{ EdgeIdKey, EdgeKey, EdgeSourceKey, EdgeSinkKey, VertexKey, VertexRemoteKey, GraphKey, GraphDirectedKey }));

	private static final int DEFAULT_BUFFER_SIZE = 8192;
	private static final Charset GML_CHARSET = Charset.forName("ISO-8859-1");
	private static final char[] WHITESPACE = new char[]{ ' ', '\n', '\r', '\t' };

	private ArrayList<KeyValuePair> _gml;
	private final List<String> _includedKeys;

	private CharBuffer _buffer;
	private boolean _endOfInput;
	private int _bufferLine;
	
	private InputStreamReader _inputStream;
	
	private MappedByteBuffer _inputBuffer;
	private final CharsetDecoder _decoder;
	
	private GMLParser(InputStream inputStream, List<String> includedKeys, int bufferSize) throws IOException {
		_gml = null;
		_includedKeys = includedKeys;

		_buffer = (CharBuffer)CharBuffer.allocate(bufferSize).flip();
		_endOfInput = false;
		_bufferLine = 1;
		
		_inputBuffer = null;
		_decoder = null;
		
		_inputStream = new InputStreamReader(inputStream, GML_CHARSET);
	}
	
	private GMLParser(File gmlFile, List<String> includedKeys, int bufferSize) throws IOException {
		_gml = null;
		_includedKeys = includedKeys;
		
		_buffer = (CharBuffer)CharBuffer.allocate(bufferSize).flip();
		_endOfInput = false;
		_bufferLine = 1;
		
		try (RandomAccessFile raf = new RandomAccessFile(gmlFile, "r")) {
			FileChannel gmlFileChannel = raf.getChannel();
			if (gmlFileChannel.size() > Integer.MAX_VALUE) {
				// TODO: 2gb limit
				throw new UnsupportedOperationException();
			}
			
			_inputBuffer = gmlFileChannel.map(MapMode.READ_ONLY, 0, gmlFileChannel.size());
			_decoder = GML_CHARSET.newDecoder();
		}
		
		_inputStream = null;
	}
	
	public static Iterable<KeyValuePair> parse(InputStream gmlStream) throws IOException {
		return parse(gmlStream, null, DEFAULT_BUFFER_SIZE);
	}
	
	public static Iterable<KeyValuePair> parse(InputStream gmlStream, List<String> includedKeys) throws IOException {
		return parse(gmlStream, includedKeys, DEFAULT_BUFFER_SIZE);
	}
	
	public static Iterable<KeyValuePair> parse(InputStream gmlStream, List<String> includedKeys, int bufferSize) throws IOException {
		GMLParser parser = new GMLParser(gmlStream, includedKeys, bufferSize);
		parser.parse();
		return parser.getGML();
	}
	
	public static Iterable<KeyValuePair> parse(File gmlFile) throws IOException {
		return parse(gmlFile, null, DEFAULT_BUFFER_SIZE);
	}
	
	public static Iterable<KeyValuePair> parse(File gmlFile, List<String> includedKeys) throws IOException {
		return parse(gmlFile, includedKeys, DEFAULT_BUFFER_SIZE);
	}
	
	public static Iterable<KeyValuePair> parse(File gmlFile, List<String> includedKeys, int bufferSize) throws IOException {
		GMLParser parser = new GMLParser(gmlFile, includedKeys, bufferSize);
		parser.parse();
		return parser.getGML();
	}

	private void parse() throws IOException {
		if (_gml != null) {
			// parse already called
			throw new IllegalStateException();
		}
		
		try {
			_gml = new ArrayList<>();
			
			for (;;) {
				String key = readKey();
				if (key == null) {
					break;
				}
				
				// avoid creating unnecessary temporaries
				if (_includedKeys == null || _includedKeys.contains(key)) {
					_gml.add(readValue(key, false));
				} else {
					readValue(key, true);
				}
			}
			_gml.trimToSize();
		} finally {
			if (_inputStream != null) {
				_inputStream.close();
			}
			
			_inputStream = null;
			_inputBuffer = null;
		}
	}
	
	public List<KeyValuePair> getGML() {
		if (_gml == null) {
			// parse hasn't been called yet
			throw new IllegalStateException();
		}
		
		return _gml;
	}

	protected String readKey() throws IOException {
		// search for key start
		int p = findTokenStart();
		if (p == -1) {
			return null;
		}
		_buffer.position(p);

		return readKeyFromCurrentPosition();
	}

	protected KeyValuePair readValue(String key, boolean ignore) throws IOException {
		// search for value start
		int p = findTokenStart();
		if (p == -1) {
			throw new GMLFormatException("Unexpected end of stream");
		}
		_buffer.position(p);

		char first = _buffer.get(_buffer.position());
		
		if (first == '[') {
			// skip list open
			_buffer.position(_buffer.position() + 1);
			
			ArrayList<KeyValuePair> list = new ArrayList<>();

			String childKey = readKey();
			while (!"]".equals(childKey)) {
				// avoid creating unnecessary temporaries
				if (!ignore && (_includedKeys == null || _includedKeys.contains(childKey))) {
					list.add(readValue(childKey, false));
				} else {
					readValue(childKey, true);
				}
				
				childKey = readKey();
				if (childKey == null) {
					throw new GMLFormatException("Unexpected end of stream");
				}
			}

			list.trimToSize();
			return new ListKeyValuePair(key, list);
		} else if (first == '"') {
			return readStringFromCurrentPosition(key, ignore);
		} else {
			// if number value
			return readNumberFromCurrentPosition(key, ignore);
		}
	}

	private int findTokenStart() throws IOException {
		int pos = _buffer.position();

		for (;;) {
			// look for non whitespace
			final int limit = _buffer.limit();
			boolean wasNewline = false;
			while (pos < limit) {
				final char c = _buffer.get(pos);
				if (!isWhitespace(c)) {
					break;
				} else if (c == '\n') {
					// count lines
					_bufferLine++;
					wasNewline = true;
				}
				
				pos++;
			}

			// found non whitespace
			if (pos < limit) {
				// skip comment lines
				if (wasNewline && _buffer.get(pos) == '#') {
					_buffer.position(pos + 1);
					pos = findNewlineStart();
					continue;
				}
				
				// return position
				return pos;
			}

			_buffer.position(pos);
			
			// if we're out of buffer, nothing we can do
			if (!canFillBuffer()) {
				return -1;
			}

			// refill buffer
			pos = fillBuffer(pos);
		}
	}
	
	private int findNewlineStart() throws IOException {
		int pos = _buffer.position();

		for (;;) {
			// look for non whitespace
			final int limit = _buffer.limit();
			while (pos < limit) {
				final char c = _buffer.get(pos);
				if (c == '\n') {
					break;
				}
				
				pos++;
			}

			// found newline
			if (pos < limit) {
				// return position
				return pos;
			}

			_buffer.position(pos);
			
			// if we're out of buffer, nothing we can do
			if (!canFillBuffer()) {
				return -1;
			}

			// refill buffer
			pos = fillBuffer(pos);
		}
	}

	private String readKeyFromCurrentPosition() throws IOException {
		int pos = _buffer.position() + 1;

		// look for whitespace
		for (;;) {
			final int limit = _buffer.limit();
			while (pos < limit) {
				final char c = _buffer.get(pos);
				if (isWhitespace(c)) {
					break;
				}
				pos++;
			}
			
			if (pos >= limit) {
				// fill, expand, or give up
				if (canFillBuffer()) {
					pos = fillBuffer(pos);
				} else if (!_endOfInput) {
					pos = expandAndFillBuffer(pos);
				} else {
					break;
				}
			} else {
				break;
			}
		}
		
		// set key end
		int oldLimit = _buffer.limit();
		_buffer.limit(pos);

		// get key
		String key = _buffer.toString();

		// reset buffer
		_buffer.limit(oldLimit);
		_buffer.position(pos);
		
		// interning key to save memory
		return key.intern();
	}
	
	private KeyValuePair readStringFromCurrentPosition(String key, boolean ignore) throws IOException {
		// skip leading quote
		_buffer.position(_buffer.position() + 1);
		int pos = _buffer.position();

		// look for trailing quote
		for (;;) {
			final int limit = _buffer.limit();
			while (pos < limit) {
				final char c = _buffer.get(pos);
				if (c == '"') {
					break;
				}
				pos++;
			}
			
			if (pos >= limit) {
				// fill, expand, or give up
				if (canFillBuffer()) {
					pos = fillBuffer(pos);
				} else if (!_endOfInput) {
					pos = expandAndFillBuffer(pos);
				} else {
					break;
				}
			} else {
				break;
			}
		}
		
		if (ignore) {
			// skip trailing quote
			_buffer.position(pos + 1);
			return null;
		} else {
			// set value end
			int oldLimit = _buffer.limit();
			_buffer.limit(pos);
	
			char[] value = new char[_buffer.remaining()];
			_buffer.get(value);
	
			// reset buffer (skip trailing quotation mark)
			_buffer.limit(oldLimit);
			_buffer.position(pos + 1);
	
			return new CharArrayKeyValuePair(key, value);
		}
	}
	
	private KeyValuePair readNumberFromCurrentPosition(String key, boolean ignore) throws IOException {
		int pos = _buffer.position() + 1;

		// look for whitespace, keeping track of whether we've seen a period (floating point number)
		boolean isFloat = false;
		for (;;) {
			int limit = _buffer.limit();
			while (pos < limit) {
				char c = _buffer.get(pos);
				if (isWhitespace(c)) {
					break;
				} else if (!isFloat && c == '.') {
					isFloat = true;
				}
				pos++;
			}
			
			if (pos >= limit) {
				// fill, expand, or give up
				if (canFillBuffer()) {
					pos = fillBuffer(pos);
				} else if (!_endOfInput) {
					pos = expandAndFillBuffer(pos);
				} else {
					break;
				}
			} else {
				break;
			}
		}
		
		if (ignore) {
			_buffer.position(pos);
			return null;
		} else {
			// set value end
			int oldLimit = _buffer.limit();
			_buffer.limit(pos);
	
			try {
				try {
					if (isFloat) {
						return new DoubleKeyValuePair(key, Double.parseDouble(_buffer.toString()));
					} else {
						long l = parseLong();
						if (l >= Integer.MIN_VALUE && l <= Integer.MAX_VALUE) {
							return new IntKeyValuePair(key, (int)l);
						} else {
							return new LongKeyValuePair(key, l);
						}
					}
				} catch (NumberFormatException e) {}
				
				// illegal value
				throw new GMLFormatException("Illegal value \"" + _buffer.toString() + "\" line " + _bufferLine);
			
			} finally {
				// reset buffer (not good use of finally block, but the simplest way to do this)
				_buffer.limit(oldLimit);
				_buffer.position(pos);
			}
		}
	}

	private long parseLong()
	{
		int pos = _buffer.position();
		
	    long value = 0;
	    
	    // check for sign
	    int sign = -1;
	    char first  = _buffer.get(pos);
	    if (first == '-')
	    {
	        pos++;
	        sign = 1;
	    }
	    else if (first == '+')
	    {
	        pos++;
	    }

	    final long MAX = (sign == -1) ? -Long.MAX_VALUE : Long.MIN_VALUE;
	    final long MULTMAX = MAX / 10;
	    
	    // parse number
	    final int limit = _buffer.limit();
	    int i = 1;
	    while (pos < limit)
	    {
	        int d = _buffer.get(pos) - '0';
	        if (d < 0 || d > 9) {
	            throw new NumberFormatException();
	        }
	        // only check for over/under flow after the 18th digit
	        if (i > 18 && (value < MULTMAX || value*10 < MAX + d)) {
	            throw new NumberFormatException();
	        }
	        value *= 10;
	        value -= d;
	        pos++;
	        i++;
	    }

	    return sign * value;
	}
	
	private boolean isWhitespace(char c) {
		for (int i = 0; i < WHITESPACE.length; i++) {
			if (c == WHITESPACE[i]) {
				return true;
			}
		}
		
		return false;
	}
	
	private int expandAndFillBuffer(int oldPosition) throws IOException {
		int delta = oldPosition - _buffer.position();
		CharBuffer tmp = (CharBuffer)CharBuffer.allocate(_buffer.capacity() * 2);
		tmp.put(_buffer);
		tmp.flip();
		_buffer = tmp;
		return fillBuffer(_buffer.position() + delta);
	}
	
	private boolean canFillBuffer() {
		return !_endOfInput && (_buffer.position() != 0 || _buffer.limit() < _buffer.capacity());
	}
	
	private int fillBuffer(int oldPosition) throws IOException {
		int delta = oldPosition - _buffer.position();
		fillBuffer();
		return _buffer.position() + delta;
	}
	
	private void fillBuffer() throws IOException {
		if (_endOfInput) {
			return;
		}

		_buffer.compact();

		if (_inputBuffer != null) {
			// fill buffer from mapped byte buffer
			
			boolean inputRemaining = _inputBuffer.hasRemaining();
			CoderResult r = _decoder.decode(_inputBuffer, _buffer, !inputRemaining);
			if (r.isUnderflow()) {
				if (inputRemaining) {
					r = _decoder.decode(_inputBuffer, _buffer, true);
				}
				
				if (r.isUnderflow()) {
					r = _decoder.flush(_buffer);
					if (r.isUnderflow()) {
						_inputBuffer = null;
						_endOfInput = true;
					}
				}
			}
			
			// handle errors
			if (r.isError()) {
				String value = String.format("0x%02X", _inputBuffer.get(_inputBuffer.position()));
				for (int i = 1; i < r.length(); i++) {
					value += String.format(" 0x%02X", _inputBuffer.get(_inputBuffer.position() + i));
				}
				throw new GMLFormatException(r + " bytes \"" + value + "\"");
			}

		} else if (_inputStream != null) {
			// fill buffer from input stream
			int r = _inputStream.read(_buffer);
			if (r == -1) {
				_endOfInput = true;
			}
		}

		_buffer.flip();
	}
	
	public static KeyValuePair getKVPForKey(Iterable<KeyValuePair> iterable, String key) {
		for (KeyValuePair kvp : iterable) {
			if (kvp.Key().equals(key)) {
				return kvp;
			}
		}

		return null;
	}

	public static KeyValuePair getKVPForKey(Iterable<KeyValuePair> iterable, String key, String default_value) {
		for (KeyValuePair kvp : iterable) {
			if (kvp.Key().equals(key)) {
				return kvp;
			}
		}

		return new StringKeyValuePair(key, default_value);
	}
	
	public static KeyValuePair getKVPForKey(Iterable<KeyValuePair> iterable, String key, long default_value) {
		for (KeyValuePair kvp : iterable) {
			if (kvp.Key().equals(key)) {
				return kvp;
			}
		}

		return new LongKeyValuePair(key, default_value);
	}
	
	public static KeyValuePair getKVPForKey(Iterable<KeyValuePair> iterable, String key, double default_value) {
		for (KeyValuePair kvp : iterable) {
			if (kvp.Key().equals(key)) {
				return kvp;
			}
		}

		return new DoubleKeyValuePair(key, default_value);
	}
	
	public static Class<? extends Object> identifyClassType(String type) {
		switch (type.toLowerCase()) {
		case StringType:
			return String.class;
		case IntegerType:
			return Integer.class;
		case LongType:
			return Long.class;
		case FloatType:
			return Float.class;
		case DoubleType:
			return Double.class;
		case BooleanType:
			return Boolean.class;
		default:
			// type was not recognized
			throw new GMLFormatException("property type \"" + type + "\" is invalid");
		}
	}

	public static Object convertGMLValueToType(Object value, Class<? extends Object> type) {
		if (type.isInstance(value)) {
			return value;
		}

		if (type.equals(String.class)) {
			return value.toString();
		} else if (type.equals(Integer.class)) {
			if (value.getClass().equals(Long.class)) {
				return ((Long)value).intValue();
			}
		} else if (type.equals(Float.class)) {
			if (value.getClass().equals(Double.class)) {
				return ((Double)value).floatValue();
			}
		} else if (type.equals(Boolean.class)) {
			if (value.getClass().equals(Long.class)) {
				return ((Long)value).longValue() == 1L;
			} else if (value.getClass().equals(Double.class)) {
				return ((Double)value).doubleValue() == 1.0d;
			}
		}

		// could not convert the value to the desired type
		throw new ClassCastException();
	}
}
