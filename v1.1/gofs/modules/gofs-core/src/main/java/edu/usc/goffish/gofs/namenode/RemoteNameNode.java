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
package edu.usc.goffish.gofs.namenode;

import java.net.*;
import java.util.*;

import javax.ws.rs.core.*;

import com.sun.jersey.api.client.*;
import com.sun.jersey.api.client.ClientResponse.Status;
import com.sun.jersey.api.client.config.*;
import com.sun.jersey.api.json.*;

import edu.usc.goffish.gofs.*;
import edu.usc.goffish.gofs.namenode.resources.LocationsResource.*;
import edu.usc.goffish.gofs.partition.*;

/**
 * This name node implementation is meant to be used in conjunction with
 * NameNodeServer. On every name node request, this class communicates over the
 * network with a NameNodeServer to satisfy the request. No state is stored
 * locally.
 */
public class RemoteNameNode implements INameNode {

	private final WebResource _baseResource;

	public RemoteNameNode(String host, int port) {
		ClientConfig clientConfig = new DefaultClientConfig();
		clientConfig.getFeatures().put(JSONConfiguration.FEATURE_POJO_MAPPING, true);
		Client c = Client.create(clientConfig);

		_baseResource = c.resource(UriBuilder.fromUri("").scheme("http").host(host).port(port).path("namenode").build());
	}

	@Override
	public List<Integer> getPartitions(String graphId) {
		if (graphId == null) {
			throw new IllegalArgumentException();
		}

		WebResource r = _baseResource.path(graphId);
		ClientResponse response = r.get(ClientResponse.class);

		Status status = response.getClientResponseStatus();
		if (Status.OK.equals(status)) {
			return response.getEntity(IntegerListWrapper.class).List;
		} else if (Status.NOT_FOUND.equals(status)) {
			return null;
		} else if (Status.BAD_REQUEST.equals(status)) {
			throw new IllegalArgumentException(response.getEntity(String.class));
		}

		throw new RuntimeException(status.toString());
	}

	@Override
	public List<Integer> getMatchingPartitions(String graphId, URI locationToMatch) {
		if (graphId == null) {
			throw new IllegalArgumentException();
		}
		if (locationToMatch == null) {
			throw new IllegalArgumentException();
		}

		WebResource r = _baseResource.path(graphId).queryParam("locationToMatch", locationToMatch.toString());
		ClientResponse response = r.get(ClientResponse.class);

		Status status = response.getClientResponseStatus();
		if (Status.OK.equals(status)) {
			return response.getEntity(IntegerListWrapper.class).List;
		} else if (Status.NOT_FOUND.equals(status)) {
			return null;
		} else if (Status.BAD_REQUEST.equals(status)) {
			throw new IllegalArgumentException(response.getEntity(String.class));
		}

		throw new RuntimeException(status.toString());
	}

	@Override
	public URI getPartitionMapping(String graphId, int partitionId) {
		if (graphId == null) {
			throw new IllegalArgumentException();
		}
		if (partitionId == BasePartition.INVALID_PARTITION) {
			throw new IllegalArgumentException();
		}

		WebResource r = _baseResource.path(graphId).path(Integer.toString(partitionId));
		ClientResponse response = r.get(ClientResponse.class);

		Status status = response.getClientResponseStatus();
		if (Status.OK.equals(status)) {
			return URI.create(response.getEntity(String.class));
		} else if (Status.NOT_FOUND.equals(status)) {
			return null;
		} else if (Status.BAD_REQUEST.equals(status)) {
			throw new IllegalArgumentException(response.getEntity(String.class));
		}

		throw new RuntimeException(status.toString());
	}

	@Override
	public boolean hasPartitionMapping(String graphId, int partitionId) {
		if (graphId == null) {
			throw new IllegalArgumentException();
		}
		if (partitionId == BasePartition.INVALID_PARTITION) {
			throw new IllegalArgumentException();
		}

		WebResource r = _baseResource.path(graphId).path(Integer.toString(partitionId));
		ClientResponse response = r.get(ClientResponse.class);

		Status status = response.getClientResponseStatus();
		if (Status.OK.equals(status)) {
			return true;
		} else if (Status.NOT_FOUND.equals(status)) {
			return false;
		} else if (Status.BAD_REQUEST.equals(status)) {
			throw new IllegalArgumentException(response.getEntity(String.class));
		}

		throw new RuntimeException(status.toString());
	}

	@Override
	public void putPartitionMapping(String graphId, int partitionId, URI location) {
		if (graphId == null) {
			throw new IllegalArgumentException();
		}
		if (partitionId == BasePartition.INVALID_PARTITION) {
			throw new IllegalArgumentException();
		}
		if (location == null) {
			throw new IllegalArgumentException();
		}

		WebResource r = _baseResource.path(graphId).path(Integer.toString(partitionId)).queryParam("location", location.toString());
		ClientResponse response = r.put(ClientResponse.class);

		Status status = response.getClientResponseStatus();
		if (Status.OK.equals(status)) {
			return;
		} else if (Status.BAD_REQUEST.equals(status)) {
			throw new IllegalArgumentException(response.getEntity(String.class));
		}

		throw new RuntimeException(status.toString());
	}

	@Override
	public void removePartitionMapping(String graphId, int partitionId) {
		if (graphId == null) {
			throw new IllegalArgumentException();
		}
		if (partitionId == BasePartition.INVALID_PARTITION) {
			throw new IllegalArgumentException();
		}

		WebResource r = _baseResource.path(graphId).path(Integer.toString(partitionId));
		ClientResponse response = r.delete(ClientResponse.class);

		Status status = response.getClientResponseStatus();
		if (Status.OK.equals(status)) {
			return;
		} else if (Status.BAD_REQUEST.equals(status)) {
			throw new IllegalArgumentException(response.getEntity(String.class));
		}

		throw new RuntimeException(status.toString());
	}
}
