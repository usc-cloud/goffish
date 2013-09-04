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
package edu.usc.goffish.gofs.partition;

import java.util.*;

import edu.usc.goffish.gofs.*;
import edu.usc.goffish.gofs.util.*;

public class InstancePropertyMap implements ISubgraphObjectProperties {

	private final long _vertexOrEdgeId;

	private final PropertySet _properties;

	private final Map<String, Object> _valueMap;

	public InstancePropertyMap(long vertexOrEdgeId, PropertySet properties) {
		_vertexOrEdgeId = vertexOrEdgeId;
		_properties = properties;

		_valueMap = new TreeMap<String, Object>();
	}

	public boolean hasSpecifiedProperties() {
		return !_valueMap.isEmpty();
	}
	
	@Override
	public long getId() {
		return _vertexOrEdgeId;
	}

	@Override
	public PropertySet getProperties() {
		return _properties;
	}
	
	@Override
	public boolean hasSpecifiedProperty(String propertyName) {
		if (!_properties.contains(propertyName)) {
			throw new IllegalArgumentException();
		}

		return _valueMap.containsKey(propertyName);
	}

	@Override
	public Collection<String> getSpecifiedProperties() {
		return Collections.unmodifiableSet(_valueMap.keySet());
	}

	@Override
	public Object getValue(String propertyName) {
		if (!_properties.contains(propertyName)) {
			throw new IllegalArgumentException();
		}

		Property property = _properties.getProperty(propertyName);

		if (property.isStatic() || !_valueMap.containsKey(propertyName)) {
			return property.getDefaults().get(_vertexOrEdgeId);
		} else {
			return _valueMap.get(propertyName);
		}
	}

	@Override
	public Iterator<String> iterator() {
		return new ReadOnlyIterator<>(_properties.propertyNames().iterator());
	}

	public void setProperty(String name, Object value) {
		Property property = _properties.getProperty(name);
		
		if (property == null) {
			throw new IllegalArgumentException();
		}

		if (!property.getType().isInstance(value)) {
			throw new IllegalArgumentException();
		}
		if (property.isStatic()) {
			throw new IllegalArgumentException();
		}

		_valueMap.put(name, value);
	}
}
