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

import java.net.*;
import java.util.*;

import javax.ws.rs.*;
import javax.ws.rs.core.*;

import org.codehaus.jettison.json.*;

import com.sun.jersey.api.client.ClientResponse.Status;

import edu.usc.goffish.gofs.namenode.*;

@Path("/" + NameNodeServer.DATA_NODES)
public class DataNodesResource {

	@GET
	@Produces({MediaType.APPLICATION_JSON})
	public JSONArray getDataNodes() {
		Collection<URI> dataNodes = NameNodeServer.getNameNode().getDataNodes();
		return new JSONArray(dataNodes);
	}
	
	@PUT
	public Response addDataNode(@QueryParam(NameNodeServer.ADD_DATA_NODE_DATANODE_QP) String dataNode) {
		try {
			NameNodeServer.getNameNode().addDataNode(new URI(dataNode));
			return Response.ok().build();
		} catch (URISyntaxException e) {
			return Response.status(Status.BAD_REQUEST).entity(e.getMessage()).build();
		}
	}
	
	@DELETE
	public Response clearDataNodes() {
		NameNodeServer.getNameNode().clearDataNodes();
		return Response.ok().build();
	}
}
