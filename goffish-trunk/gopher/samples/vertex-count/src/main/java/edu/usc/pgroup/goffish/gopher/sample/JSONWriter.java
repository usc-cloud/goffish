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
package edu.usc.pgroup.goffish.gopher.sample;

import edu.usc.goffish.gofs.ISubgraphInstance;
import edu.usc.goffish.gofs.ISubgraphObjectProperties;
import edu.usc.goffish.gofs.ITemplateEdge;
import edu.usc.goffish.gofs.ITemplateVertex;
import edu.usc.goffish.gopher.api.GopherSubGraph;
import edu.usc.goffish.gopher.api.SubGraphMessage;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;
import java.util.Set;

/**
 * Convert the GoFS graph to JSON format.
 *
 * @author alok kumbhare
 */
public class JSONWriter extends GopherSubGraph {


    private PrintWriter templateWriter;
    private PrintWriter instanceWriter;
    private Set<String> vertexProperties;
    private Set<String> edgeProperties;

    @Override
    public void compute(List<SubGraphMessage> subGraphMessages) {

        try{
            if (getSuperStep() == 0) {


                vertexProperties = partition.getVertexProperties().propertyNames();
                edgeProperties = partition.getEdgeProperties().propertyNames();

                for (ITemplateVertex vertex : subgraph.vertices()) {
                    //System.out.print("Vid:"+vertex.getId());
                    writeTemplate(partition.getId(), subgraph.getId(), vertex);
                }

                templateWriter.flush();
                templateWriter.close();
                //System.out.println("Template done for " + subgraph.getId());


                    Iterable<? extends ISubgraphInstance> instances = subgraph.getInstances(Long.MIN_VALUE, Long.MAX_VALUE, partition.getVertexProperties(), partition.getEdgeProperties(), false);

                    for(ISubgraphInstance instance : instances)
                    {

                        for (ITemplateVertex vertex : subgraph.vertices()) {
                            writeInstance(partition.getId(), subgraph.getId(), instance, vertex);
                        }

                        //System.out.println("Instance " + instance.getId() + " Done");
                        instanceWriter.flush();
                        instanceWriter.close();
                        instanceWriter = null;
                    }


            }
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            System.out.println("Error:" + e.getMessage());
        }
        finally {
            voteToHalt();
        }
    }

    private void writeTemplate(int partId, long subId, ITemplateVertex vertex) {
        try {
            if(templateWriter == null)
            {
                templateWriter = new PrintWriter(new FileWriter(partId + "_" + subId + ".txt", false));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        long vertexId = vertex.getId();
        Iterable<? extends ITemplateEdge> edges = vertex.outEdges();

        String vertexValue = "{";
        int vpcnt = 0;
        //System.out.println("vertices start");
        for(String vproperty: vertexProperties)
        {
            String propValue = "";
            if(partition.getVertexProperties().getProperty(vproperty)!=null && partition.getVertexProperties().getProperty(vproperty).getDefaults() !=null && partition.getVertexProperties().getProperty(vproperty).getDefaults().get(vertexId)!=null)
            {
                propValue = partition.getVertexProperties().getProperty(vproperty).getDefaults().get(vertexId).toString();
                if(vpcnt > 0){ vertexValue += ","; }
                else
                {
                    vpcnt++;
                }
                vertexValue += "\"" + vproperty + "\" : " + "\"" + propValue + "\"";
                //System.out.println(vertexValue);
            }
        }
        vertexValue += "}";
        //System.out.println("vertices done");
        String edgesValues = "";
        //System.out.println("edges start");
        int edgeCnt = 0;
        for(ITemplateEdge edge: edges)
        {
            if(edgeCnt > 0){ edgesValues += ","; }
            else
            {
                edgeCnt++;
            }
            edgesValues += "[";
            edgesValues += edge.getSink(vertex).getId() + ",";
            edgesValues += "{";
            int epcnt = 0;
            for(String eproperty: edgeProperties)
            {
                String propValue = "";
                if(partition.getEdgeProperties().getProperty(eproperty)!=null && partition.getEdgeProperties().getProperty(eproperty).getDefaults() != null && partition.getEdgeProperties().getProperty(eproperty).getDefaults().get(vertexId)!=null)
                {
                    propValue = partition.getEdgeProperties().getProperty(eproperty).getDefaults().get(edge.getId()).toString();
                    if(epcnt > 0){ edgesValues += ","; }
                    else
                    {
                        epcnt++;
                    }
                    edgesValues += "\"" + eproperty + "\" : " + "\"" + propValue + "\"";
                }
            }
            edgesValues += "}";
            edgesValues += "]";
            //System.out.println(edgesValues);
        }
        //edgesValues += "]";
        templateWriter.printf("[%s,%s,[%s]]\n",vertexId,vertexValue,edgesValues);
        //System.out.printf("[%s,%s,[%s]]",vertexId,vertexValue,edgesValues);
    }

    private void writeInstance(int partId, long subId, ISubgraphInstance instance,
                               ITemplateVertex vertex){
        try {
            if(instanceWriter == null)
            {
                instanceWriter = new PrintWriter(new FileWriter(instance.getId() + "_" + partId + "_" + subId  + ".txt", false));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        long vertexId = vertex.getId();
        String vertexValue = "{";
        int vpcnt = 0;

        ISubgraphObjectProperties vProperties = instance.getPropertiesForVertex(vertexId);
        for(String vproperty: vProperties)
        {
            if(vProperties.getValue(vproperty) != null)
            {
                String propValue = vProperties.getValue(vproperty).toString();
                if(vpcnt > 0){ vertexValue += ","; }
                else
                {
                    vpcnt++;
                }
                vertexValue += "\"" + vproperty + "\" : " + "\"" + propValue + "\"";
            }
        }
        vertexValue += "}";

        String edgesValues = "";
        int edgeCnt = 0;
        for(ITemplateEdge edge: vertex.outEdges())
        {
            if(edgeCnt > 0){ edgesValues += ","; }
            else
            {
                edgeCnt++;
            }
            edgesValues += "[";
            edgesValues += edge.getSink(vertex).getId() + ",";
            edgesValues += "{";
            int epcnt = 0;
            ISubgraphObjectProperties eProperties = instance.getPropertiesForEdge(edge.getId());
            for(String eproperty: eProperties)
            {
                if(eProperties.getValue(eproperty) != null)
                {
                    String propValue = eProperties.getValue(eproperty).toString();
                    if(epcnt > 0){ edgesValues += ","; }
                    else
                    {
                        epcnt++;
                    }
                    edgesValues += "\"" + eproperty + "\" : " + "\"" + propValue + "\"";
                }
            }
            edgesValues += "}";
            edgesValues += "]";
        }

        instanceWriter.printf("[%s,%s,[%s]]\n",vertexId,vertexValue,edgesValues);
    }
}