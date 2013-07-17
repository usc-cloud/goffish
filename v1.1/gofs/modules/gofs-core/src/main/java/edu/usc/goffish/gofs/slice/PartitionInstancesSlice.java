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
package edu.usc.goffish.gofs.slice;

import java.io.*;
import java.util.*;

import edu.usc.goffish.gofs.*;

class PartitionInstancesSlice implements ISlice {

	private static final long serialVersionUID = 1L;

	private transient final UUID _id;

	public final Map<Long, PropertyInstanceTuple> InstancesMap;

	public PartitionInstancesSlice(Property property, Iterable<? extends ISubgraphInstance> instances, boolean isVertexProperty) {
		if (property == null) {
			throw new IllegalArgumentException();
		}
		if (instances == null) {
			throw new IllegalArgumentException();
		}
		
		_id = UUID.randomUUID();

		// load instances
		// TODO: we need to add kryo serializer for fastutils collections, until then, use java collection
		TreeMap<Long, PropertyInstanceTuple> instancesMap = new TreeMap<>();
		for (ISubgraphInstance instance : instances) {
			Collection<? extends ISubgraphObjectProperties> properties = (isVertexProperty ? instance.getPropertiesForVertices() : instance.getPropertiesForEdges());

			// create object properties map
			// TODO: we need to add kryo serializer for fastutils collections, until then, use java collection
			HashMap<Long, Object> instanceValues = new HashMap<>(properties.size(), 1f);
			for (ISubgraphObjectProperties objectProperties : properties) {
				if (objectProperties.hasSpecifiedProperty(property.getName())) {
					instanceValues.put(objectProperties.getId(), objectProperties.getValue(property.getName()));
				}
			}
			
			// create tuple
			PropertyInstanceTuple tuple = new PropertyInstanceTuple(instance.getTimestampStart(), instance.getTimestampEnd(), Collections.unmodifiableMap(instanceValues));

			// add tuple to map and adjust timespan info
			instancesMap.put(instance.getId(), tuple);
		}

		InstancesMap = Collections.unmodifiableMap(instancesMap);
	}

	@Override
	public UUID getId() {
		return _id;
	}

	static class PropertyInstanceTuple implements Serializable {

		private static final long serialVersionUID = 1L;

		public final long TimespanStart;
		public final long TimespanEnd;
		public final Map<Long, Object> InstanceValues;

		public PropertyInstanceTuple(long timespanStart, long timespanEnd, Map<Long, Object> instanceValues) {
			TimespanStart = timespanStart;
			TimespanEnd = timespanEnd;
			InstanceValues = instanceValues;
		}
	}
}
