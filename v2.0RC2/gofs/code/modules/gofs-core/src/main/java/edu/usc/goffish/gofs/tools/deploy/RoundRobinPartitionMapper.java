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

package edu.usc.goffish.gofs.tools.deploy;

import java.net.*;
import java.util.*;

import edu.usc.goffish.gofs.partition.*;

public class RoundRobinPartitionMapper implements IPartitionMapper {

	private final Map<Integer, URI> _mappings;
	
	private final List<URI> _locations;
	private int _counter;
	
	public RoundRobinPartitionMapper(Collection<URI> locations) {
		if (locations == null || locations.isEmpty()) {
			throw new IllegalArgumentException();
		}
		
		_mappings = new HashMap<>();
		_locations = new ArrayList<>(locations);
		_counter = 0;
	}
	
	@Override
	public URI getLocationForPartition(ISerializablePartition partition) {
		URI location = _mappings.get(partition.getId());
		if (location == null) {
			location = _locations.get(_counter++ % _locations.size());
			_mappings.put(partition.getId(), location);
			
			System.out.println("partition " + partition.getId() + " mapped to " + location);
		}
		
		return location;
	}
}
