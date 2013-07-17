/*
 *  Copyright 2013 University of Southern California
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.package edu.usc.goffish.gopher.sample;
 */
package edu.usc.goffish.tools.json.service;


import edu.usc.goffish.gofs.*;
import edu.usc.goffish.gofs.slice.SliceManager;
import edu.usc.goffish.tools.json.GraphKeyWords;
import edu.usc.goffish.tools.json.ResourceHolder;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;

import javax.annotation.Resource;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.ext.Provider;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Resource
@Provider
@Path("/GoFS")
public class GoFSRestAPI {




    @GET
    @Produces("text/plain")
    public String welcomeMessage()
    {
        // Return some cliched textual content
        return "GOFS Rest API is up and Running";
    }

    @GET
    @Path("/simpleJSON")
    @Produces(MediaType.APPLICATION_JSON)
    public String simpleJSON() {

        return "{\n" +
                "    \"graphs\": [\n" +
                "        {\n" +
                "            \"directed\": 1,\n" +
                "            \"nodes\": [\n" +
                "                {\n" +
                "                    \"id\": 1,\n" +
                "                    \"label\": \"los angeles\"\n" +
                "                },\n" +
                "                {\n" +
                "                    \"id\": 2,\n" +
                "                    \"label\": \"bakesfield\"\n" +
                "                }\n" +
                "            ],\n" +
                "            \"edges\": [\n" +
                "                {\n" +
                "                    \"id\": 1,\n" +
                "                    \"source\": 1,\n" +
                "                    \"target\": 2,\n" +
                "                    \"label\": \"Interstate 5\",\n" +
                "                    \"distance_miles\": 83,\n" +
                "                    \"lanes\": 2\n" +
                "                }\n" +
                "            ]\n" +
                "        }\n" +
                "    ]\n" +
                "}";
    }


    @Path("/SubgraphIds")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public String getSubgraphIds() {

        IPartition partition = ResourceHolder.getInstance().getPartition();

        List<String> subs = new ArrayList<String>();
        for(ISubgraph subgraph : partition) {
            subs.add(Long.toString(subgraph.getId()));
        }

        return new JSONArray(subs).toString();

    }


    @Path("/Subgraph/id={id}/startTime={startTime}/duration={duration}")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public String getSubgraph(@PathParam("id")String id ,@PathParam("startTime")String startTime ,
                              @PathParam("duration")String duration) throws Exception {



        IPartition partition = ResourceHolder.getInstance().getPartition();
        ISubgraph subgraph  = partition.getSubgraph(Long.parseLong(id));

        SliceManager sliceManager = ResourceHolder.getInstance().getSliceManager();

        long st = Long.parseLong(startTime);
        long et = Long.parseLong(startTime) + Long.parseLong(duration);

        Iterable<ISubgraphInstance> subs = sliceManager.readInstances(subgraph,st,et,subgraph.getVertexProperties(),subgraph.getEdgeProperties());

        JSONObject outer = new JSONObject();

        JSONArray graphs = new JSONArray();

        for(ISubgraphInstance instance : subs) {
            JSONObject graph = new JSONObject();
            graph.put(GraphKeyWords.ID,instance.getId());
            graph.put(GraphKeyWords.TIME_START,instance.getTimestampStart());
            graph.put(GraphKeyWords.DURATION,(instance.getTimestampEnd() -
                    instance.getTimestampStart()));

            JSONArray nodes = new JSONArray();

            for(TemplateVertex vertex : instance.getTemplate().vertices()) {
                System.out.println("Vert");
                JSONObject node = new JSONObject();
                node.put(GraphKeyWords.ID,vertex.getId());

                ISubgraphObjectProperties properties = instance.
                        getPropertiesForVertex(vertex.getId());

                for(String key : properties) {

                    node.put(key,properties.getValue(key));

                }
                nodes.put(node);
            }

            graph.put(GraphKeyWords.NODES,nodes);


            JSONArray edges = new JSONArray();

            for(TemplateEdge edge : instance.getTemplate().edges()) {
                System.out.println("edge");
                JSONObject e = new JSONObject();
                e.put(GraphKeyWords.ID,edge.getId());
                e.put(GraphKeyWords.SOURCE,edge.getSource().getId());
                e.put(GraphKeyWords.TARTGET,edge.getSink().getId());


                ISubgraphObjectProperties properties = instance.getPropertiesForEdge(edge.getId());

                for(String key : properties) {
                    e.put(key,properties.getValue(key));
                }

                edges.put(e);

            }

            graph.put(GraphKeyWords.EDGES,edges);

            graphs.put(graph);

        }

        outer.put(GraphKeyWords.GRAPHS,graphs);
        return outer.toString();
    }



    @Path("/test/id={id}")
    @GET
    public void test(@PathParam("id")String id) throws IOException {
        IPartition partition = ResourceHolder.getInstance().getPartition();
        ISubgraph subgraph  = partition.getSubgraph(Long.parseLong(id));

        SliceManager sliceManager = ResourceHolder.getInstance().getSliceManager();

        long st = Long.MIN_VALUE;
        long et = Long.MAX_VALUE;
        Iterable<ISubgraphInstance> subs = sliceManager.readInstances(subgraph,st,et,
                subgraph.getVertexProperties(),subgraph.getEdgeProperties());

        System.out.println("SGraph id : " + id);
        for(ISubgraphInstance subgraphInstance : subs) {

            System.out.println("Start : " + subgraphInstance.getTimestampStart());

            System.out.println("Duration : " + (subgraphInstance.getTimestampEnd()-
                    subgraphInstance.getTimestampStart()));
            System.out.println("*******************************************************");
        }

    }







    }
