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

package edu.usc.goffish.gofs.namenode.resources;

import it.unimi.dsi.fastutil.ints.*;

import java.net.*;

import javax.ws.rs.*;
import javax.ws.rs.core.*;
import javax.ws.rs.core.Response.Status;

import org.codehaus.jettison.json.*;

import edu.usc.goffish.gofs.namenode.*;

@Path("/" + NameNodeServer.DIRECTORY)
public class DirectoryResource {

	@Path("")
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public Response getGraphs() {
		return Response.ok(new JSONArray(NameNodeServer.getNameNode().getGraphs())).build();
	}
	
	@Path("/{graphId}")
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public Response getGraphPartitions(@PathParam("graphId") String graphId, @QueryParam(NameNodeServer.GET_GRAPH_PARTITIONS_LOCATIONTOMATCH_QP) String locationToMatch) {
		try {
			IntCollection partitions;
			if (locationToMatch == null) {
				partitions = NameNodeServer.getNameNode().getPartitions(graphId);
			} else {
				partitions = NameNodeServer.getNameNode().getMatchingPartitions(graphId, new URI(locationToMatch));
			}

			if (partitions != null) {
				return Response.ok(new JSONArray(partitions)).build();
			} else {
				return Response.status(Status.NOT_FOUND).build();
			}
		} catch (IllegalArgumentException e) {
			return Response.status(Status.BAD_REQUEST).entity(e.getMessage()).build();
		} catch (URISyntaxException e) {
			return Response.status(Status.BAD_REQUEST).entity(e.getMessage()).build();
		}
	}

	@Path("/{graphId}/{partitionId}")
	@GET
	public Response getPartitionMapping(@PathParam("graphId") String graphId, @PathParam("partitionId") int partitionId) {
		try {
			URI location = NameNodeServer.getNameNode().getPartitionMapping(graphId, partitionId);
			if (location != null) {
				return Response.ok(location.toString()).build();
			} else {
				return Response.status(Status.NOT_FOUND).build();
			}
		} catch (IllegalArgumentException e) {
			return Response.status(Status.BAD_REQUEST).entity(e.getMessage()).build();
		}
	}

	@Path("/{graphId}/{partitionId}")
	@PUT
	public Response putPartitionMapping(@PathParam("graphId") String graphId, @PathParam("partitionId") int partitionId, @QueryParam(NameNodeServer.PUT_PARTITION_MAPPING_LOCATION_QP) String location) {
		try {
			if (location == null) {
				return Response.status(Status.BAD_REQUEST).build();
			}

			NameNodeServer.getNameNode().putPartitionMapping(graphId, partitionId, new URI(location));
			return Response.ok().build();
		} catch (IllegalArgumentException e) {
			return Response.status(Status.BAD_REQUEST).entity(e.getMessage()).build();
		} catch (URISyntaxException e) {
			return Response.status(Status.BAD_REQUEST).entity(e.getMessage()).build();
		}
	}

	@Path("/{graphId}/{partitionId}")
	@DELETE
	public Response removePartitionMapping(@PathParam("graphId") String graphId, @PathParam("partitionId") int partitionId) {
		try {
			NameNodeServer.getNameNode().removePartitionMapping(graphId, partitionId);
			return Response.ok().build();
		} catch (IllegalArgumentException e) {
			return Response.status(Status.BAD_REQUEST).entity(e.getMessage()).build();
		}
	}
}
