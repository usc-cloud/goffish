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

public class TemplateRemoteVertex extends TemplateVertex {

	private final int _remotePartitionId;
	private long _remoteSubgraphId;

	public TemplateRemoteVertex(long id, int remotePartitionId) {
		this(id, remotePartitionId, BaseSubgraph.INVALID_SUBGRAPH);
	}

	public TemplateRemoteVertex(long id, int remotePartitionId, long remoteSubgraphId) {
		super(id);

		if (remotePartitionId == Partition.INVALID_PARTITION) {
			throw new IllegalArgumentException();
		}

		_remotePartitionId = remotePartitionId;
		_remoteSubgraphId = remoteSubgraphId;
	}

	@Override
	public boolean isRemote() {
		return true;
	}

	@Override
	public int getRemotePartitionId() {
		return _remotePartitionId;
	}

	@Override
	public long getRemoteSubgraphId() {
		return _remoteSubgraphId;
	}

	// TODO: should this be part of the API?
	public void setRemoteSubgraphId(long remoteSubgraphId) {
		if (_remoteSubgraphId != BaseSubgraph.INVALID_SUBGRAPH) {
			throw new IllegalStateException();
		}

		_remoteSubgraphId = remoteSubgraphId;
	}
}
