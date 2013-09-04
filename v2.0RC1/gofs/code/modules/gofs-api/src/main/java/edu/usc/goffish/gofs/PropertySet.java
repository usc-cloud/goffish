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

/**
 * This class represents a set of properties and supports collection oriented methods over those properties. The primary
 * reason for this class's existence is to maintain a guarantee of immutability over the set of properties so that the
 * collection can be passed around with necessitating copying.
 */
public final class PropertySet extends AbstractSet<Property> {

	public static final PropertySet EmptyPropertySet = new PropertySet(Collections.<Property>emptyList());

	private final Map<String, Property> _properties;

	public PropertySet(Collection<? extends Property> properties) {
		if (properties == null) {
			throw new IllegalArgumentException();
		}

		TreeMap<String, Property> ps = new TreeMap<>();
		for (Property property : properties) {
			ps.put(property.getName(), property);
		}
		_properties = Collections.unmodifiableMap(ps);
	}

	/**
	 * Returns whether this property set contains a property with the given name.
	 * 
	 * @param propertyName
	 *            the property name to query for
	 * @return true if this property set contains a property with the given name, false otherwise
	 */
	public boolean contains(String propertyName) {
		return _properties.containsKey(propertyName);
	}

	@Override
	public boolean contains(Object o) {
		if (!(o instanceof Property)) {
			return false;
		}

		return contains((Property)o);
	}

	/**
	 * Returns whether this property set contains the given property.
	 * 
	 * @param propertyName
	 *            the property name to query for
	 * @return true if this property set contains the given property, false otherwise
	 */
	public boolean contains(Property property) {
		if (property == null) {
			return false;
		}

		return contains(property.getName());
	}

	@Override
	public int size() {
		return _properties.size();
	}

	@Override
	public Iterator<Property> iterator() {
		return _properties.values().iterator();
	}

	/**
	 * Returns an unmodifiable view of the names of the properties within this set.
	 * 
	 * @return an unmodifiable view of the names of the properties within this set
	 */
	public Set<String> propertyNames() {
		return _properties.keySet();
	}

	/**
	 * Returns the property with the given name in this set.
	 * 
	 * @param propertyName
	 *            the property name to query for
	 * @return the property with the given name in this set, or null if no such property is in this set
	 */
	public Property getProperty(String propertyName) {
		return _properties.get(propertyName);
	}

	/**
	 * Returns a subset of this property set containing only the properties specified in the input property set.
	 * 
	 * @param properties
	 *            the properties to retain in the subset
	 * @return a subset of this property set
	 */
	public PropertySet subset(PropertySet properties) {
		if (properties == null) {
			throw new IllegalArgumentException();
		}

		if (properties.isEmpty()) {
			return EmptyPropertySet;
		}

		ArrayList<Property> subProperties = new ArrayList<>(Math.min(size(), properties.size()));
		for (Property property : properties) {
			if (_properties.containsKey(property.getName())) {
				subProperties.add(property);
			}
		}

		if (subProperties.isEmpty()) {
			return EmptyPropertySet;
		}

		return new PropertySet(subProperties);
	}

	/**
	 * Returns a subset of this property set containing only the properties specified in the input collection.
	 * 
	 * @param properties
	 *            the properties to retain in the subset
	 * @return a subset of this property set
	 */
	public PropertySet subset(Collection<String> properties) {
		if (properties == null) {
			throw new IllegalArgumentException();
		}

		if (properties.isEmpty()) {
			return EmptyPropertySet;
		}

		ArrayList<Property> subProperties = new ArrayList<>(properties.size());
		for (String property : properties) {
			if (_properties.containsKey(property)) {
				subProperties.add(_properties.get(property));
			}
		}

		if (subProperties.isEmpty()) {
			return EmptyPropertySet;
		}

		return new PropertySet(subProperties);
	}

	/**
	 * Returns a superset of this property set containing all the properties in this property set and those in the input
	 * property set.
	 * 
	 * @param properties
	 *            the properties to add in the superset
	 * @return a superset of this property set
	 */
	public PropertySet superset(Collection<Property> additional) {
		if (additional == null) {
			throw new IllegalArgumentException();
		}

		if (additional.isEmpty()) {
			return this;
		}

		ArrayList<Property> superProperties = new ArrayList<>(size() + additional.size());
		superProperties.addAll(this);
		superProperties.addAll(additional);
		return new PropertySet(superProperties);
	}

	@Override
	public String toString() {
		return _properties.values().toString();
	}
}
