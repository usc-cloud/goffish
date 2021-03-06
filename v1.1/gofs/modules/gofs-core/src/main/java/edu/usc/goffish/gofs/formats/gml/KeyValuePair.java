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

import java.lang.ref.*;
import java.util.*;

import org.apache.commons.lang3.*;

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

	public abstract Class<? extends Object> getValueType();
	
	// TODO: removable
	public abstract Object Value();
	
	public abstract Iterable<KeyValuePair> ValueAsList();

	public abstract String ValueAsString();

	public abstract long ValueAsLong();

	public abstract double ValueAsDouble();
	
	@Override
	public String toString() {
		return _key + ": " + ValueAsString();
	}
	
	@SuppressWarnings("unchecked")
	public static KeyValuePair createKVP(String key, Object value) {
		if (value.getClass() == String.class) {
			return new StringKeyValuePair(key, (String)value);
		} else if (value.getClass() == String.class) {
			return new LongKeyValuePair(key, (Long)value);
		} else if (value.getClass() == String.class) {
			return new DoubleKeyValuePair(key, (Double)value);
		} else if (value.getClass() == String.class) {
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
	public final Class<? extends Object> getValueType() {
		return String.class;
	}
	
	@Override
	public final Object Value() {
		return ValueAsString();
	}
	
	@Override
	public final List<KeyValuePair> ValueAsList() {
		throw new ClassCastException();
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
			value = StringEscapeUtils.unescapeHtml4(value);
		}
		_cachedString = new SoftReference<String>(value);
		return value;
	}
	
	public final char[] ValueAsCharArray() {
		return _value;
	}

	@Override
	public final long ValueAsLong() {
		throw new ClassCastException();
	}

	@Override
	public final double ValueAsDouble() {
		throw new ClassCastException();
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
	public final Class<? extends Object> getValueType() {
		return String.class;
	}
	
	@Override
	public final Object Value() {
		return _value;
	}

	@Override
	public final List<KeyValuePair> ValueAsList() {
		throw new ClassCastException();
	}

	@Override
	public final String ValueAsString() {
		return _value;
	}

	@Override
	public final long ValueAsLong() {
		throw new ClassCastException();
	}

	@Override
	public final double ValueAsDouble() {
		throw new ClassCastException();
	}
}

final class LongKeyValuePair extends KeyValuePair {

	private final long _value;
	
	public LongKeyValuePair(String key, long value) {
		super(key);
		
		_value = value;
	}

	@Override
	public final Class<? extends Object> getValueType() {
		return Long.class;
	}
	
	@Override
	public final Object Value() {
		return _value;
	}

	@Override
	public final List<KeyValuePair> ValueAsList() {
		throw new ClassCastException();
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
	public final Class<? extends Object> getValueType() {
		return Long.class;
	}
	
	@Override
	public final Object Value() {
		return _value;
	}

	@Override
	public final List<KeyValuePair> ValueAsList() {
		throw new ClassCastException();
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
	public final Class<? extends Object> getValueType() {
		return Double.class;
	}
	
	@Override
	public final Object Value() {
		return _value;
	}

	@Override
	public final List<KeyValuePair> ValueAsList() {
		throw new ClassCastException();
	}

	@Override
	public final String ValueAsString() {
		return Double.toString(_value);
	}

	@Override
	public final long ValueAsLong() {
		throw new ClassCastException();
	}

	@Override
	public final double ValueAsDouble() {
		return _value;
	}
}

final class ListKeyValuePair extends KeyValuePair {

	private Iterable<KeyValuePair> _value;
	
	protected ListKeyValuePair(String key, Iterable<KeyValuePair> value) {
		super(key);
		
		if (value == null) {
			throw new IllegalArgumentException();
		}
		
		_value = value;
	}

	@Override
	public final Class<? extends Object> getValueType() {
		return List.class;
	}
	
	@Override
	public final Object Value() {
		return _value;
	}

	@Override
	public final Iterable<KeyValuePair> ValueAsList() {
		return _value;
	}

	@Override
	public final String ValueAsString() {
		throw new ClassCastException();
	}

	@Override
	public final long ValueAsLong() {
		throw new ClassCastException();
	}

	@Override
	public final double ValueAsDouble() {
		throw new ClassCastException();
	}
	
	@Override
	public final String toString() {
		return Key() + ": [...]";
	}
}