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
package edu.usc.goffish.gofs.namenode.resources;

import java.net.*;
import java.util.*;

import javax.ws.rs.*;
import javax.ws.rs.core.*;
import javax.ws.rs.core.Response.Status;
import javax.xml.bind.annotation.*;

import edu.usc.goffish.gofs.namenode.*;

@Path("/namenode")
public class LocationsResource {

	@Path("/{graphId}")
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public Response getGraphPartitions(@PathParam("graphId") String graphId, @QueryParam("locationToMatch") String locationToMatch) {
		try {
			List<Integer> partitions;
			if (locationToMatch == null) {
				partitions = NameNodeServer.getNameNode().getPartitions(graphId);
			} else {
				partitions = NameNodeServer.getNameNode().getMatchingPartitions(graphId, new URI(locationToMatch));
			}
			
			if (partitions != null) {
				return Response.ok(new IntegerListWrapper(partitions)).build();
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
	public Response getPartitionMapping(@PathParam("graphId") String graphId, @PathParam("partitionId") int partitionId)  {
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
	public Response putPartitionMapping(@PathParam("graphId") String graphId, @PathParam("partitionId") int partitionId, @QueryParam("location") String location) {
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
	
	@XmlRootElement
	public static class IntegerListWrapper {
		
		public final List<Integer> List;
		
		public IntegerListWrapper() {
			List = null;
		}
		
		public IntegerListWrapper(List<Integer> list) {
			List = list;
		}
	}
}
