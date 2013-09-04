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

package edu.usc.goffish.gofs.slice;

import it.unimi.dsi.fastutil.longs.*;
import it.unimi.dsi.fastutil.objects.*;

import java.util.*;

import com.esotericsoftware.kryo.*;

import edu.usc.goffish.gofs.*;
import edu.usc.goffish.gofs.partition.*;
import edu.usc.goffish.gofs.slice.InstancesMetadata.InstanceTuple;

final class PartitionInstancesSlice implements ISlice {

	private static final long serialVersionUID = -8456896263185595555L;

	private transient final UUID _id;

	@NotNull
	private final Map<InstanceTuple, Long2ObjectOpenHashMap<Object>> _instancesValues;

	PartitionInstancesSlice() {
		_id = UUID.randomUUID();
		_instancesValues = new HashMap<>();
	}

	public boolean isEmpty() {
		return _instancesValues.isEmpty();
	}
	
	public void addInstance(InstanceTuple tuple, ISubgraphInstance instance) {
		Collection<? extends ISubgraphObjectProperties> propertyValues = tuple.IsVertexProperty ? instance.getPropertiesForVertices() : instance.getPropertiesForEdges();
		Long2ObjectOpenHashMap<Object> values = new Long2ObjectOpenHashMap<>(128, 1f);
		for (ISubgraphObjectProperties objectProperties : propertyValues) {
			if (!objectProperties.hasSpecifiedProperty(tuple.Property)) {
				continue;
			}

			values.put(objectProperties.getId(), objectProperties.getValue(tuple.Property));
		}

		_instancesValues.put(tuple, values);
	}

	@Override
	public UUID getId() {
		return _id;
	}

	public boolean fillInstance(long subgraphId, String property, boolean isVertexProperty, long instanceId,  SliceInstance instance) {
		return fillInstance(new InstanceTuple(subgraphId, property, isVertexProperty, instanceId), instance);
	}
	
	boolean fillInstance(InstanceTuple tuple, SliceInstance instance) {
		Long2ObjectOpenHashMap<Object> propertyValues = _instancesValues.get(new InstanceTuple(tuple.SubgraphId, tuple.Property, tuple.IsVertexProperty, tuple.InstanceId));
		if (propertyValues == null) {
			return false;
		}
		
		ObjectIterator<Long2ObjectMap.Entry<Object>> it = propertyValues.long2ObjectEntrySet().fastIterator();
		while (it.hasNext()) {
			Long2ObjectMap.Entry<Object> entry = it.next();
			InstancePropertyMap propertyMap = tuple.IsVertexProperty ? instance.getPropertiesForVertexForAdd(entry.getLongKey()) : instance.getPropertiesForEdgeForAdd(entry.getLongKey());
			propertyMap.setProperty(tuple.Property, entry.getValue());
		}

		return true;
	}
	
	@Override
	public String toString() {
		StringBuilder s = new StringBuilder("Entries: " + _instancesValues.size() + "\n");
		for (Map.Entry<InstanceTuple, Long2ObjectOpenHashMap<Object>> entry : _instancesValues.entrySet()) {
			InstanceTuple it = entry.getKey();
			s.append("  Subgraph: " + it.SubgraphId + ", Property: " + it.Property + ", Instance: " + it.InstanceId + "\n");
		}
		
		return s.toString();
	}
}
