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
import java.nio.*;
import java.nio.charset.*;
import java.util.*;

final class GMLParser {

	public static final String GRAPH_KEY = "graph";
	public static final String GRAPH_DIRECTED_KEY = "directed";
	public static final String GRAPH_VERTEX_PROPERTIES_KEY = "vertex_properties";
	public static final String GRAPH_EDGE_PROPERTIES_KEY = "edge_properties";
	public static final String GRAPH_PROPERTY_IS_STATIC = "is_static";
	public static final String GRAPH_PROPERTY_TYPE = "type";
	public static final String GRAPH_INSTANCE_ID_KEY = "id";
	public static final String GRAPH_INSTANCE_TIMESTAMP_START_KEY = "timestamp_start";
	public static final String GRAPH_INSTANCE_TIMESTAMP_END_KEY = "timestamp_end";
	public static final String VERTEX_KEY = "node";
	public static final String VERTEX_ID_KEY = "id";
	public static final String VERTEX_REMOTE_KEY = "remote";
	public static final String EDGE_KEY = "edge";
	public static final String EDGE_ID_KEY = "id";
	public static final String EDGE_SOURCE_KEY = "source";
	public static final String EDGE_SINK_KEY = "target";

	public static final String STRING_TYPE = "string";
	public static final String INTEGER_TYPE = "integer";
	public static final String LONG_TYPE = "long";
	public static final String FLOAT_TYPE = "float";
	public static final String DOUBLE_TYPE = "double";
	public static final String BOOLEAN_TYPE = "boolean";
	public static final String LIST_TYPE = "list";

	public static final List<String> VERTEX_KEYS = Collections.unmodifiableList(Arrays.asList(new String[]{ VERTEX_ID_KEY, VERTEX_REMOTE_KEY }));
	public static final List<String> EDGE_KEYS = Collections.unmodifiableList(Arrays.asList(new String[]{ EDGE_ID_KEY, EDGE_SOURCE_KEY, EDGE_SINK_KEY }));
	public static final List<String> GRAPH_KEYS = Collections.unmodifiableList(Arrays.asList(new String[]{ EDGE_ID_KEY, EDGE_KEY, EDGE_SOURCE_KEY, EDGE_SINK_KEY, VERTEX_KEY, VERTEX_REMOTE_KEY, GRAPH_KEY, GRAPH_DIRECTED_KEY }));

	private static final int DEFAULT_BUFFER_SIZE = 16384;
	static final Charset GML_CHARSET = StandardCharsets.UTF_8;

	private static boolean isWhitespace(char c) {
		return c == ' ' || c == '\n' || c == '\r' || c == '\t';
	}
	
	private ArrayList<KeyValuePair> _gml;
	private final List<String> _includedKeys;

	private CharBuffer _buffer;
	private boolean _endOfInput;
	private int _bufferLine;
	
	private InputStreamReader _inputStream;
	
	private GMLParser(InputStream inputStream, List<String> includedKeys, int bufferSize) throws IOException {
		_gml = null;
		_includedKeys = includedKeys;

		_buffer = (CharBuffer)CharBuffer.allocate(bufferSize).flip();
		_endOfInput = false;
		_bufferLine = 1;
		
		_inputStream = new InputStreamReader(inputStream, GML_CHARSET);
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
				_inputStream = null;
			}
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
						long l = parseLongFromCurrentPosition();
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

	private long parseLongFromCurrentPosition()
	{
		int pos = _buffer.position();
	    final int limit = _buffer.limit();
		
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
		
		int length = limit - pos;
		if (length > 19) {
			throw new NumberFormatException();
		}
	    
	    long value = 0;
	    
	    if (length < 19) {
	    	// don't need to worry about over/underflow
	    	while (pos < limit) {
		        int d = _buffer.get(pos) - '0';
		        if (d < 0 || d > 9) {
		            throw new NumberFormatException();
		        }
		        value *= 10;
		        value -= d;
		        pos++;
		    }
	    } else {
	    	// check for over/underflow for 19th digit only
		    final long MAX = (sign == -1) ? -Long.MAX_VALUE : Long.MIN_VALUE;
		    final long MULTMAX = MAX / 10;
		    
		    while (pos < limit) {
		        int d = _buffer.get(pos) - '0';
		        if (d < 0 || d > 9) {
		            throw new NumberFormatException();
		        }
		        // only check for over/under flow after the 18th digit
		        if (length < 1 && (value < MULTMAX || value*10 < MAX + d)) {
		            throw new NumberFormatException();
		        }
		        value *= 10;
		        value -= d;
		        pos++;
		        length--;
		    }
	    }

	    return sign * value;
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

		// fill buffer from input stream
		if (_inputStream.read(_buffer) == -1) {
			_endOfInput = true;
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
		case STRING_TYPE:
			return String.class;
		case INTEGER_TYPE:
			return Integer.class;
		case LONG_TYPE:
			return Long.class;
		case FLOAT_TYPE:
			return Float.class;
		case DOUBLE_TYPE:
			return Double.class;
		case BOOLEAN_TYPE:
			return Boolean.class;
		case LIST_TYPE:
			return List.class;
		default:
			// type was not recognized
			throw new GMLFormatException("property type \"" + type + "\" is invalid");
		}
	}

	public static Object convertGMLValueToType(Object value, Class<? extends Object> type) {
		if (type.equals(List.class) && value instanceof List) {
			@SuppressWarnings("unchecked")
			List<KeyValuePair> children = (List<KeyValuePair>)value;
			ArrayList<Object> list = new ArrayList<>(children.size());
			for (KeyValuePair kvp : children) {
				list.add(kvp.Value());
			}
			return list;
		}
		
		if (type.isInstance(value)) {
			return value;
		}

		if (type.equals(String.class)) {
			return value.toString();
		} else if (type.equals(Integer.class)) {
			if (value instanceof Number) {
				return ((Number)value).intValue();
			}
		} else if (type.equals(Float.class)) {
			if (value instanceof Number) {
				return ((Number)value).floatValue();
			}
		} else if (type.equals(Boolean.class)) {
			if (value instanceof Number) {
				return ((Number)value).intValue() == 1;
			}
		}

		// could not convert the value to the desired type
		throw new ClassCastException();
	}
}
