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

import java.util.*;

import com.esotericsoftware.kryo.*;

final class PartitionMetadataSlice implements ISlice {

	private static final long serialVersionUID = -2688323522980200155L;

	private final transient UUID _sliceId;

	private final int _partitionId;
	@NotNull
	private final UUID _partitionSlice;
	@NotNull
	private final InstancesMetadata _instancesMetadata;

	PartitionMetadataSlice(UUID partitionMetadataSliceId, int partitionId, UUID partitionSlice, InstancesMetadata instancesMetadata) {
		if (partitionMetadataSliceId == null) {
			throw new IllegalArgumentException();
		}
		if (instancesMetadata == null) {
			throw new IllegalArgumentException();
		}

		_sliceId = partitionMetadataSliceId;
		_partitionId = partitionId;
		_partitionSlice = partitionSlice;
		_instancesMetadata = instancesMetadata;
	}

	@Override
	public UUID getId() {
		return _sliceId;
	}

	public int getPartitionId() {
		return _partitionId;
	}

	public UUID getPartitionSlice() {
		return _partitionSlice;
	}

	public InstancesMetadata getInstancesMetadata() {
		return _instancesMetadata;
	}
}
