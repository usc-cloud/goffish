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
package edu.usc.goffish.gofs.json.resources;

import java.io.*;
import java.util.*;

import javax.annotation.*;
import javax.ws.rs.*;
import javax.ws.rs.core.*;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.ext.*;

import org.codehaus.jackson.*;
import org.codehaus.jackson.JsonGenerator.Feature;
import org.codehaus.jettison.json.*;

import edu.usc.goffish.gofs.*;
import edu.usc.goffish.gofs.tools.*;

@Resource
@Provider
@Path("/gofsjson")
public class GoFSJSONResource {
	
	@Path("/subgraphIds")
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public Response getSubgraphIds() {

		IPartition partition = GoFSJSONServer.getPartition();

		JSONArray subgraphIdList = new JSONArray();
		for(ISubgraph subgraph : partition) {
			subgraphIdList.put(subgraph.getId());
		}

		return Response.ok(subgraphIdList).build();
	}

	@Path("/subgraph/{subgraphId}/numVertices")
	@GET
	@Produces(MediaType.TEXT_PLAIN)
	public Response numVertices(@PathParam("subgraphId")long subgraphId){
		ISubgraph subgraph = GoFSJSONServer.getPartition().getSubgraph(subgraphId);
		if(subgraph != null)
			return Response.ok(Integer.toString(subgraph.numVertices())).build();

		return Response.status(Status.BAD_REQUEST).build();
	}

	private JsonGenerator createJsonGenerator(OutputStream output)
			throws IOException {
		JsonFactory jsonFactory = new JsonFactory();
		JsonGenerator jsonGenerator = jsonFactory.createJsonGenerator(output);
		jsonGenerator.enable(Feature.AUTO_CLOSE_TARGET);
		jsonGenerator.useDefaultPrettyPrinter();
		return jsonGenerator;
	}

	@Path("/graphProperties")
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public Response getGraphProperties() throws Exception {
		StreamingOutput streamingOutput = new StreamingOutput() {

			@Override
			public void write(OutputStream output) throws IOException {

				IPartition partition = GoFSJSONServer.getPartition();
				JsonGenerator jsonGenerator = createJsonGenerator(output);
				jsonGenerator.writeStartObject();

				PropertySet vertexProperties = partition.getVertexProperties();
				writeVertexPropertiesJSONObject(jsonGenerator, vertexProperties);

				PropertySet edgeProperties = partition.getEdgeProperties();
				writeEdgePropertiesJSONObject(jsonGenerator, edgeProperties);
				jsonGenerator.writeEndObject();

				//Close the stream
				jsonGenerator.close();
			}
		};

		return Response.ok(streamingOutput).build();
	}

	@Path("/subgraph/{subgraphId}/timestampRange")
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public Response getTimestampRange(@PathParam("subgraphId") final long subgraphId){
		IPartition partition = GoFSJSONServer.getPartition();
		final ISubgraph subgraph  = partition.getSubgraph(subgraphId);

		//BAD_REQUEST Response
		if(subgraph == null){
			return Response.status(Status.BAD_REQUEST).build();
		}

		StreamingOutput streamingOutput = new StreamingOutput() {

			@Override
			public void write(OutputStream output) throws IOException,
			WebApplicationException {
				long startTime = 0;
				long endTime = 0;
				Iterator<? extends ISubgraphInstance> forwardInstanceIterator = subgraph.getInstances(Long.MIN_VALUE, Long.MAX_VALUE, subgraph.getVertexProperties(), subgraph.getEdgeProperties(), false).iterator();
				if(forwardInstanceIterator.hasNext()){
					startTime = forwardInstanceIterator.next().getTimestampStart();
				}

				Iterator<? extends ISubgraphInstance> backwardInstanceIterator = subgraph.getInstances(Long.MIN_VALUE, Long.MAX_VALUE, subgraph.getVertexProperties(), subgraph.getEdgeProperties(), true).iterator();
				if(backwardInstanceIterator.hasNext()){
					endTime = forwardInstanceIterator.next().getTimestampEnd();
				}

				JsonGenerator jsonGenerator = createJsonGenerator(output);
				jsonGenerator.writeStartObject();
				jsonGenerator.writeNumberField(JSONConstants.TIME_START, startTime);
				jsonGenerator.writeNumberField(JSONConstants.TIME_END, endTime);
				jsonGenerator.writeEndObject();

				jsonGenerator.close();
			}
		};

		return Response.ok(streamingOutput).build();
	}

	@Path("/subgraph/{subgraphId}")
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public Response getSubgraph(@PathParam("subgraphId") long subgraphId ,@QueryParam("starttime") long startTime ,
			@QueryParam("duration") long duration, @QueryParam("vertexProperties")List<String> filterVertexProperties, @QueryParam("edgeProperties") List<String> filterEdgeProperties) throws IOException {
		IPartition partition = GoFSJSONServer.getPartition();
		ISubgraph subgraph  = partition.getSubgraph(subgraphId);

		//BAD_REQUEST Response
		if(subgraph == null){
			return Response.status(Status.BAD_REQUEST).build();
		}

		//Get the vertex and edge properties based on filter queries
		PropertySet vertexPropertySet = subgraph.getVertexProperties();
		PropertySet edgePropertySet = subgraph.getEdgeProperties();
		if(filterVertexProperties != null && !filterVertexProperties.isEmpty()){
			vertexPropertySet = getFilterPropertySet(vertexPropertySet, filterVertexProperties);
		}

		if(filterEdgeProperties != null && !filterEdgeProperties.isEmpty()){
			edgePropertySet = getFilterPropertySet(edgePropertySet, filterEdgeProperties);
		}

		//Read the instances
		long endTime;
		if(startTime <=0 || duration <= 0){
			startTime = Long.MIN_VALUE;
			endTime = Long.MAX_VALUE;
		}else{
			endTime = startTime + duration;
		}
		final Iterable<? extends ISubgraphInstance> instanceCollections = subgraph.getInstances(startTime, endTime,
				vertexPropertySet, edgePropertySet, false);

		//NO_CONTENT Response, if instances are not fetched for the query
		if(instanceCollections == null){
			return Response.status(Status.NO_CONTENT).build();
		}

		//Write JSON Streaming
		StreamingOutput streamingOutput = new StreamingOutput() {

			@Override
			public void write(OutputStream output) throws IOException,
			WebApplicationException {
				JsonGenerator jsonGenerator = createJsonGenerator(output);
				writeGraphInstanceListJSONObject(jsonGenerator, instanceCollections);
				jsonGenerator.close();
			}
		};

		return Response.ok(streamingOutput).build();
	}

	private PropertySet getFilterPropertySet(PropertySet propertySet, List<String> filterProperties){
		Collection<Property> propertyCollection = new ArrayList<>();
		for(String property : filterProperties){
			propertyCollection.add(propertySet.getProperty(property));
		}

		return new PropertySet(propertyCollection);
	}

	private void writeGraphInstanceListJSONObject(JsonGenerator jsonGenerator, 
			Iterable<? extends ISubgraphInstance> instanceCollections)
					throws IOException {
		jsonGenerator.writeStartObject();
		jsonGenerator.writeArrayFieldStart(JSONConstants.GRAPHS);
		for(ISubgraphInstance instance : instanceCollections) {
			writeGraphInstanceJSONObject(jsonGenerator, instance);
		}
		jsonGenerator.writeEndArray();
		jsonGenerator.writeEndObject();
	}

	private void writeGraphInstanceJSONObject(JsonGenerator jsonGenerator, ISubgraphInstance instance)
			throws IOException {
		jsonGenerator.writeStartObject();
		jsonGenerator.writeNumberField(JSONConstants.ID, instance.getId());
		jsonGenerator.writeNumberField(JSONConstants.TIME_START, instance.getTimestampStart());
		jsonGenerator.writeNumberField(JSONConstants.DURATION, (instance.getTimestampEnd() -
				instance.getTimestampStart()));
		jsonGenerator.writeArrayFieldStart(JSONConstants.NODES);
		Iterable<? extends ITemplateVertex> vertexCollection = instance.getTemplate().vertices();
		ISubgraphObjectProperties vertexProperties;
		Object value = null;
		//Write the vertex properties
		for(ITemplateVertex templateVertex : vertexCollection) {
			jsonGenerator.writeStartObject();
			jsonGenerator.writeNumberField(JSONConstants.ID, templateVertex.getId());

			vertexProperties = instance.
					getPropertiesForVertex(templateVertex.getId());
			for(String key : vertexProperties) {
				value = vertexProperties.getValue(key);
				if(value != null)
					writeObjectField(jsonGenerator, key, value);
			}
			jsonGenerator.writeEndObject();
		}
		jsonGenerator.writeEndArray();

		jsonGenerator.writeArrayFieldStart(JSONConstants.EDGES);
		Iterable<? extends ITemplateEdge> edgeCollection = instance.getTemplate().edges();
		ISubgraphObjectProperties edgeProperties;
		//Write the edge properties
		for(ITemplateEdge templateEdge : edgeCollection) {
			jsonGenerator.writeStartObject();
			jsonGenerator.writeNumberField(JSONConstants.ID, templateEdge.getId());
			jsonGenerator.writeNumberField(JSONConstants.SOURCE, templateEdge.getSource().getId());
			jsonGenerator.writeNumberField(JSONConstants.TARGET, templateEdge.getSink().getId());

			edgeProperties = instance.getPropertiesForEdge(templateEdge.getId());
			for(String key : edgeProperties) {
				value = edgeProperties.getValue(key);
				if(value != null)
					writeObjectField(jsonGenerator, key, value);
			}

			jsonGenerator.writeEndObject();
		}
		jsonGenerator.writeEndArray();

		jsonGenerator.writeEndObject();
	}

	private void writeObjectField(JsonGenerator jsonGenerator, String key, Object value)
			throws JsonGenerationException, IOException{
		if(isListType(value)){
			writeObjectArrayField(jsonGenerator, key, (List<?>) value);
		}else{
			jsonGenerator.writeObjectField(key, value);
		}
	}

	private boolean isListType(Object value){
		return value instanceof List ? true : false;
	}

	private void writeObjectArrayField(JsonGenerator jsonGenerator, String key, List<?> listValues)
			throws JsonGenerationException, IOException{
		jsonGenerator.writeArrayFieldStart(key);
		for(Object value : listValues){
			jsonGenerator.writeObject(value);
		}
		jsonGenerator.writeEndArray();
	}

	private void writeEdgePropertiesJSONObject(
			JsonGenerator jsonGenerator, PropertySet edgeProperties)
					throws IOException, JsonGenerationException {
		jsonGenerator.writeArrayFieldStart(JSONConstants.EDGE_PROPERTIES);
		for(Property property : edgeProperties){
			writePropertyJSONObject(jsonGenerator, property);
		}
		jsonGenerator.writeEndArray();
	}

	public void writeVertexPropertiesJSONObject(
			JsonGenerator jsonGenerator, PropertySet vertexProperties)
					throws IOException, JsonGenerationException {
		jsonGenerator.writeArrayFieldStart(JSONConstants.VERTEX_PROPERTIES);
		for(Property property : vertexProperties){
			writePropertyJSONObject(jsonGenerator, property);
		}
		jsonGenerator.writeEndArray();
	}

	public void writePropertyJSONObject(JsonGenerator jsonGenerator,
			Property property) throws IOException,
			JsonGenerationException {
		jsonGenerator.writeStartObject();
		jsonGenerator.writeObjectFieldStart(property.getName());
		jsonGenerator.writeBooleanField(JSONConstants.IS_STATIC, property.isStatic());
		jsonGenerator.writeStringField(JSONConstants.TYPE, property.getType().getSimpleName().toLowerCase());
		jsonGenerator.writeEndObject();
		jsonGenerator.writeEndObject();
	}

	/**
	 * JSON Constants
	 */
	private class JSONConstants {
		public static final String ID = "id";
		public static final String GRAPHS = "graphs";
		public static final String TIME_START = "time_start";
		public static final String TIME_END = "time_end";
		public static final String DURATION = "duration";
		public static final String NODES = "nodes";
		public static final String EDGES = "edges";
		public static final String SOURCE = "source";
		public static final String TARGET = "target";
		public static final String IS_STATIC = "is_static";
		public static final String TYPE = "type";
		public static final String VERTEX_PROPERTIES = "node_properties";
		public static final String EDGE_PROPERTIES = "edge_properties";
		
	}
}
