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
import java.lang.ref.*;
import java.util.*;

import org.apache.commons.lang.*;

abstract class KeyValuePair {

	private final String _key;

	protected KeyValuePair(String key) {
		if (key == null) {
			throw new IllegalArgumentException();
		}

		_key = key;
	}

	public final String Key() {
		return _key;
	}

	public abstract Object Value();

	public Iterable<KeyValuePair> ValueAsList() {
		throw new ClassCastException();
	}

	public String ValueAsString() {
		return Value().toString();
	}

	public long ValueAsLong() {
		throw new ClassCastException();
	}

	public double ValueAsDouble() {
		throw new ClassCastException();
	}

	public void write(Writer output) throws IOException {
		output.write(_key);
		output.write(" ");
		output.write(ValueAsString());
	}

	@Override
	public String toString() {
		return _key + ": " + ValueAsString();
	}

	@SuppressWarnings("unchecked")
	public static KeyValuePair createKVP(String key, Object value) {
		if (value.getClass() == String.class) {
			return new StringKeyValuePair(key, (String)value);
		} else if (value.getClass() == Long.class) {
			return new LongKeyValuePair(key, (Long)value);
		} else if (value.getClass() == Integer.class) {
			return new IntKeyValuePair(key, (Integer)value);
		} else if (value.getClass() == Double.class) {
			return new DoubleKeyValuePair(key, ((Number)value).doubleValue());
		} else if (value.getClass() == List.class) {
			return new ListKeyValuePair(key, (List<KeyValuePair>)value);
		} else {
			// unknown value type
			throw new ClassCastException();
		}
	}
}

final class CharArrayKeyValuePair extends KeyValuePair {

	private final char[] _value;
	private SoftReference<String> _cachedString;

	public CharArrayKeyValuePair(String key, char[] value) {
		super(key);

		if (value == null) {
			throw new IllegalArgumentException();
		}

		_value = value;
		_cachedString = null;
	}

	@Override
	public final Object Value() {
		return ValueAsString();
	}

	@Override
	public final String ValueAsString() {
		if (_cachedString != null) {
			String s = _cachedString.get();
			if (s != null) {
				return s;
			}
		}

		boolean foundAmpersand = false;
		for (int i = 0; i < _value.length; i++) {
			if (_value[i] == '&') {
				foundAmpersand = true;
				break;
			}
		}

		String value = new String(_value);
		if (foundAmpersand) {
			value = StringEscapeUtils.unescapeHtml(value);
		}
		_cachedString = new SoftReference<String>(value);
		return value;
	}

	@Override
	public final void write(Writer output) throws IOException {
		output.write(Key());
		output.write(" \"");
		output.write(_value);
		output.write("\"");
	}
}

final class StringKeyValuePair extends KeyValuePair {

	private final String _value;

	public StringKeyValuePair(String key, String value) {
		super(key);

		if (value == null) {
			throw new IllegalArgumentException();
		}

		_value = value;
	}

	@Override
	public final Object Value() {
		return _value;
	}

	@Override
	public final void write(Writer output) throws IOException {
		output.write(Key());
		output.write(" \"");
		output.write(StringEscapeUtils.escapeHtml(_value));
		output.write("\"");
	}
}

final class LongKeyValuePair extends KeyValuePair {

	private final long _value;

	public LongKeyValuePair(String key, long value) {
		super(key);

		_value = value;
	}

	@Override
	public final Object Value() {
		return _value;
	}

	@Override
	public final String ValueAsString() {
		return Long.toString(_value);
	}

	@Override
	public final long ValueAsLong() {
		return _value;
	}

	@Override
	public final double ValueAsDouble() {
		return _value;
	}
}

final class IntKeyValuePair extends KeyValuePair {

	private final int _value;

	public IntKeyValuePair(String key, int value) {
		super(key);

		_value = value;
	}

	@Override
	public final Object Value() {
		return ValueAsLong();
	}

	@Override
	public final String ValueAsString() {
		return Integer.toString(_value);
	}

	@Override
	public final long ValueAsLong() {
		return _value;
	}

	@Override
	public final double ValueAsDouble() {
		return _value;
	}
}

final class DoubleKeyValuePair extends KeyValuePair {

	private final double _value;

	public DoubleKeyValuePair(String key, double value) {
		super(key);

		_value = value;
	}

	@Override
	public final Object Value() {
		return ValueAsDouble();
	}

	@Override
	public final String ValueAsString() {
		return Double.toString(_value);
	}

	@Override
	public final double ValueAsDouble() {
		return _value;
	}
}

final class ListKeyValuePair extends KeyValuePair {

	private List<KeyValuePair> _value;

	protected ListKeyValuePair(String key, List<KeyValuePair> value) {
		super(key);

		if (value == null) {
			throw new IllegalArgumentException();
		}

		_value = value;
	}

	@Override
	public final Object Value() {
		return ValueAsList();
	}

	@Override
	public final List<KeyValuePair> ValueAsList() {
		return _value;
	}

	@Override
	public final void write(Writer output) throws IOException {
		output.write(Key());
		output.write(" [");
		for (KeyValuePair kvp : _value) {
			output.write(" ");
			kvp.write(output);
		}
		output.write(" ]");
	}

	@Override
	public final String toString() {
		return Key() + ": " + _value.toString();
	}
}