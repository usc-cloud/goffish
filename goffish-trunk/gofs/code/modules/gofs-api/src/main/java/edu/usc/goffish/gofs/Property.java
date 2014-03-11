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

package edu.usc.goffish.gofs;

import java.util.*;

import it.unimi.dsi.fastutil.longs.*;

/**
 * This class represents a single property that an instance might contain values for. A property may be either static or
 * dynamic. If the property is static, it will have a single given value on a per vertex/edge basis, and no per instance
 * values may be specified to override this. If the property is dynamic it may have defaults specified on a per
 * vertex/edge basis, but instance values will override these defaults.
 */
public final class Property {

	private final String _name;
	private final Class<? extends Object> _type;
	private final boolean _isStatic;
	private final Long2ObjectMap<Object> _defaultValues;

	public Property(String name, Class<? extends Object> type, boolean isStatic, Map<Long, Object> defaultValues) {
		this(name, type, isStatic, new Long2ObjectOpenHashMap<>(defaultValues), false);
	}

	public Property(String name, Class<? extends Object> type, boolean isStatic, Long2ObjectMap<Object> defaultValues, boolean defensiveCopyDefaultValues) {
		if (name == null) {
			throw new IllegalArgumentException();
		}
		if (type == null) {
			throw new IllegalArgumentException();
		}
		if (defaultValues == null) {
			throw new IllegalArgumentException();
		}
		if (isStatic && defaultValues.isEmpty()) {
			throw new IllegalArgumentException();
		}
		for (Object o : defaultValues.values()) {
			if (o == null || !type.isInstance(o)) {
				throw new IllegalArgumentException();
			}
		}

		_name = name;
		_type = type;
		_isStatic = isStatic;

		if (defensiveCopyDefaultValues) {
			_defaultValues = Long2ObjectMaps.unmodifiable(new Long2ObjectOpenHashMap<>(defaultValues));
		} else {
			_defaultValues = Long2ObjectMaps.unmodifiable(defaultValues);
		}
	}

	/**
	 * Returns the name of this property.
	 * 
	 * @return the name of this property
	 */
	public String getName() {
		return _name;
	}

	/**
	 * Returns the type of any values for this property.
	 * 
	 * @return the type of any values for this property
	 */
	public Class<? extends Object> getType() {
		return _type;
	}

	/**
	 * Returns whether this property is static or dynamic.
	 * 
	 * @return true if this is a static property, false if this is a dynamic property
	 */
	public boolean isStatic() {
		return _isStatic;
	}

	/**
	 * Returns a map of vertex/edge id to default value at that vertex/edge.
	 * 
	 * @return a map of vertex/edge id to default value at that vertex/edge
	 */
	public Long2ObjectMap<Object> getDefaults() {
		return _defaultValues;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + _name.hashCode();
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;

		Property other = (Property)obj;
		return _name.equals(other._name);
	}

	@Override
	public String toString() {
		return "Property[name=" + _name + ", type=" + _type.getSimpleName() + ", static=" + _isStatic + "]";
	}
}
