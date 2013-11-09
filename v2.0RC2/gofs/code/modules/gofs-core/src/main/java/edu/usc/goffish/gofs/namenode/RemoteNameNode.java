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

package edu.usc.goffish.gofs.namenode;

import it.unimi.dsi.fastutil.ints.*;

import java.net.*;
import java.util.*;

import org.codehaus.jettison.json.*;

import com.sun.jersey.api.client.*;
import com.sun.jersey.api.client.ClientResponse.Status;
import com.sun.jersey.api.client.config.*;
import com.sun.jersey.api.json.*;

import edu.usc.goffish.gofs.*;
import edu.usc.goffish.gofs.partition.*;
import edu.usc.goffish.gofs.slice.*;

/**
 * This name node implementation is meant to be used in conjunction with NameNodeServer. On every name node request,
 * this class communicates over the network with a NameNodeServer to satisfy the request. No state is stored locally.
 */
public class RemoteNameNode implements IInternalNameNode, IPartitionDirectory {

	private final WebResource _baseResource;

	public RemoteNameNode(URI uri) {
		ClientConfig clientConfig = new DefaultClientConfig();
		clientConfig.getFeatures().put(JSONConfiguration.FEATURE_POJO_MAPPING, true);
		Client c = Client.create(clientConfig);

		_baseResource = c.resource(uri);
	}

	@Override
	public URI getURI() {
		return _baseResource.getURI();
	}

	@Override
	public boolean isAvailable() {
		try {
			WebResource r = _baseResource.path(NameNodeServer.AVAILABLE);
			ClientResponse response = r.get(ClientResponse.class);

			Status status = response.getClientResponseStatus();
			if (Status.OK.equals(status)) {
				return true;
			}
		} catch (Exception e) {
		}

		return false;
	}

	@Override
	public void addDataNode(URI dataNode) {
		if (dataNode == null) {
			throw new IllegalArgumentException();
		}

		WebResource r = _baseResource.path(NameNodeServer.DATA_NODES).queryParam(NameNodeServer.ADD_DATA_NODE_DATANODE_QP, dataNode.toString());
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
	public void clearDataNodes() {
		WebResource r = _baseResource.path(NameNodeServer.DATA_NODES);
		ClientResponse response = r.delete(ClientResponse.class);

		Status status = response.getClientResponseStatus();
		if (Status.OK.equals(status)) {
			return;
		}

		throw new RuntimeException(status.toString());
	}

	@Override
	public Set<URI> getDataNodes() {
		WebResource r = _baseResource.path(NameNodeServer.DATA_NODES);
		ClientResponse response = r.get(ClientResponse.class);

		Status status = response.getClientResponseStatus();
		if (Status.OK.equals(status)) {
			JSONArray array = response.getEntity(JSONArray.class);
			try {
				HashSet<URI> uris = new HashSet<>(array.length(), 1f);
				for (int i = 0; i < array.length(); i++) {
					uris.add(URI.create(array.getString(i)));
				}
				return uris;
			} catch (JSONException e) {
				throw new IllegalStateException(e);
			}
		} else if (Status.BAD_REQUEST.equals(status)) {
			throw new IllegalArgumentException(response.getEntity(String.class));
		}

		throw new RuntimeException(status.toString());
	}

	@Override
	public ISliceSerializer getSerializer() {
		WebResource r = _baseResource.path(NameNodeServer.SERIALIZER);
		ClientResponse response = r.get(ClientResponse.class);

		Status status = response.getClientResponseStatus();
		if (Status.OK.equals(status)) {
			String serializerTypeName = response.getEntity(String.class);
			try {
				return SliceSerializerProvider.loadSliceSerializer(serializerTypeName);
			} catch (ReflectiveOperationException e) {
				throw new RuntimeException(e);
			}
		} else if (Status.NOT_FOUND.equals(status)) {
			return null;
		}

		throw new RuntimeException(status.toString());
	}

	@Override
	public void setSerializer(Class<? extends ISliceSerializer> sliceSerializer) {
		if (sliceSerializer == null) {
			throw new IllegalArgumentException();
		}

		WebResource r = _baseResource.path(NameNodeServer.SERIALIZER).queryParam(NameNodeServer.SET_SERIALIZER_SERIALIZERTYPE_QP, sliceSerializer.getName());
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
	public IPartitionDirectory getPartitionDirectory() {
		return this;
	}

	@Override
	public Collection<String> getGraphs() {
		WebResource r = _baseResource.path(NameNodeServer.DIRECTORY);
		ClientResponse response = r.get(ClientResponse.class);

		Status status = response.getClientResponseStatus();
		if (Status.OK.equals(status)) {
			JSONArray array = response.getEntity(JSONArray.class);
			ArrayList<String> graphs = new ArrayList<>(array.length());
			try {
				for (int i = 0; i < array.length(); i++) {
					graphs.add(array.getString(i));
				}
			} catch (JSONException e) {
				throw new IllegalStateException(e);
			}
			return graphs;
		}

		throw new RuntimeException(status.toString());
	}

	@Override
	public IntCollection getPartitions(String graphId) {
		if (graphId == null) {
			throw new IllegalArgumentException();
		}

		WebResource r = _baseResource.path(NameNodeServer.DIRECTORY).path(graphId);
		ClientResponse response = r.get(ClientResponse.class);

		Status status = response.getClientResponseStatus();
		if (Status.OK.equals(status)) {
			JSONArray array = response.getEntity(JSONArray.class);
			IntArrayList partitions = new IntArrayList(array.length());
			try {
				for (int i = 0; i < array.length(); i++) {
					partitions.add(array.getInt(i));
				}
			} catch (JSONException e) {
				throw new IllegalStateException(e);
			}
			return partitions;
		} else if (Status.NOT_FOUND.equals(status)) {
			return null;
		} else if (Status.BAD_REQUEST.equals(status)) {
			throw new IllegalArgumentException(response.getEntity(String.class));
		}

		throw new RuntimeException(status.toString());
	}

	@Override
	public IntCollection getMatchingPartitions(String graphId, URI locationToMatch) {
		if (graphId == null) {
			throw new IllegalArgumentException();
		}
		if (locationToMatch == null) {
			throw new IllegalArgumentException();
		}

		WebResource r = _baseResource.path(NameNodeServer.DIRECTORY).path(graphId).queryParam(NameNodeServer.GET_GRAPH_PARTITIONS_LOCATIONTOMATCH_QP, locationToMatch.toString());
		ClientResponse response = r.get(ClientResponse.class);

		Status status = response.getClientResponseStatus();
		if (Status.OK.equals(status)) {
			JSONArray array = response.getEntity(JSONArray.class);
			IntArrayList partitions = new IntArrayList(array.length());
			try {
				for (int i = 0; i < array.length(); i++) {
					partitions.add(array.getInt(i));
				}
			} catch (JSONException e) {
				throw new IllegalStateException(e);
			}
			return partitions;
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
		if (partitionId == Partition.INVALID_PARTITION) {
			throw new IllegalArgumentException();
		}

		WebResource r = _baseResource.path(NameNodeServer.DIRECTORY).path(graphId).path(Integer.toString(partitionId));
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
		if (partitionId == Partition.INVALID_PARTITION) {
			throw new IllegalArgumentException();
		}

		WebResource r = _baseResource.path(NameNodeServer.DIRECTORY).path(graphId).path(Integer.toString(partitionId));
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
		if (partitionId == Partition.INVALID_PARTITION) {
			throw new IllegalArgumentException();
		}
		if (location == null) {
			throw new IllegalArgumentException();
		}

		WebResource r = _baseResource.path(NameNodeServer.DIRECTORY).path(graphId).path(Integer.toString(partitionId)).queryParam(NameNodeServer.PUT_PARTITION_MAPPING_LOCATION_QP, location.toString());
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
		if (partitionId == Partition.INVALID_PARTITION) {
			throw new IllegalArgumentException();
		}

		WebResource r = _baseResource.path(NameNodeServer.DIRECTORY).path(graphId).path(Integer.toString(partitionId));
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
