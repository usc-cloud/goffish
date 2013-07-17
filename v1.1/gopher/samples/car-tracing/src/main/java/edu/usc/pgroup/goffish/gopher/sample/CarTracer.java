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
 *   * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package edu.usc.pgroup.goffish.gopher.sample;

import edu.usc.goffish.gofs.ISubgraphInstance;
import edu.usc.goffish.gofs.ISubgraphObjectProperties;
import edu.usc.goffish.gofs.TemplateEdge;
import edu.usc.goffish.gofs.TemplateVertex;
import edu.usc.goffish.gopher.api.GopherSubGraph;
import edu.usc.goffish.gopher.api.SubGraphMessage;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.Iterator;
import java.util.List;

public class CarTracer extends GopherSubGraph {

    private PrintWriter writer;
    private static final String LICENCE_LIST = "license_list";
    private static final String DISTANCE = "dist";
    public CarTracer() {
        try {
            writer = new PrintWriter(new File("path.txt"));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void compute(List<SubGraphMessage> subGraphMessages) {

        for (SubGraphMessage message : subGraphMessages) {
            String data = new String(message.getData());

            String[] parts = data.split(",");




            //Data format : nodeId,endTime,lid
            if ("NILL".equals(parts[0])) {
                //Check whether this lid is in a vertex
                //if not in the subgraph halt
                //if in this subgraph log and look above until hit a remote vertex
                //if neighbors are remote then send messages

                long endTime = Long.parseLong(parts[1]);
                String licenceId = parts[2].trim();

                try {

                    Iterable<? extends ISubgraphInstance> instances = subgraph.getInstances(
                            endTime - 5 * 60 * 1000, endTime, subgraph.getVertexProperties(),
                            subgraph.getEdgeProperties(), false);



                    Iterator<? extends ISubgraphInstance> subIt = instances.iterator();
                    if (subIt.hasNext()) {
                        ISubgraphInstance instance = subIt.next();
                        boolean containsId = false;
                        for (TemplateVertex vertex : subgraph.getTemplate().vertices()) {
                            ISubgraphObjectProperties properties = instance.getPropertiesForVertex(vertex.getId());
                            String lids = (String) properties.getValue(LICENCE_LIST);
                            String lidParts[] = lids.split(",");
                            boolean hasLid = false;
                            for (String id : lidParts) {
                                if (id.equals(licenceId)) {
                                    hasLid = true;
                                    break;
                                }
                            }

                            if (hasLid) {
                                containsId = true;
                                logPath(vertex.getId(),licenceId,endTime);
                                long startTime;
                                TemplateVertex lastKnownV = vertex;
                                while (true) {
                                    startTime = endTime;
                                    endTime = startTime + 5*60*1000;
                                    instances = subgraph.getInstances(startTime, endTime,
                                            subgraph.getVertexProperties(), subgraph.getEdgeProperties(), false);
                                    subIt = instances.iterator();
                                    if(subIt.hasNext()) {
                                        instance = subIt.next();

                                        //Check in local sink nodes
                                        boolean foundInLocal = false;
                                        for(TemplateEdge edge : lastKnownV.outEdges()) {
                                            TemplateVertex sink = edge.getSink();
                                            if(sink.getId() == lastKnownV.getId()) {
                                                sink = edge.getSource();
                                            }

                                            if(sink != null && !subgraph.isRemoteVertex(sink.getId())) {
                                                ISubgraphObjectProperties props = instance.getPropertiesForVertex(sink.getId());
                                                String ids = (String)props.getValue(LICENCE_LIST);
                                                String[] idParts = ids.split(",");


                                                boolean found = false;
                                                for (String id : idParts) {
                                                    if (id.equals(licenceId)) {
                                                        found = true;

                                                        break;
                                                    }
                                                }

                                                if (found) {
                                                    foundInLocal = true;
                                                    logPath(sink.getId(),licenceId,endTime);
                                                    lastKnownV = sink;

                                                    break;
                                                }

                                            }
                                        }


                                        if(!foundInLocal) {
                                            for(TemplateEdge edge : lastKnownV.outEdges()) {
                                                TemplateVertex sink = edge.getSink();
                                                if(sink.getId() == lastKnownV.getId()) {
                                                    sink = edge.getSource();
                                                }

                                                if(sink != null && subgraph.isRemoteVertex(sink.getId())) {
                                                    String payLoad = "" + sink.getId() + "," + (endTime +5*60*1000)
                                                            + "," + licenceId;
                                                    SubGraphMessage msg = new SubGraphMessage(payLoad.getBytes());
                                                    msg.addTargetVertex(sink.getId());
                                                    sentMessage(subgraph.getPartitionForRemoteVertex(sink.getId()),msg);
                                                }

                                            }
                                            voteToHalt();
                                            break;
                                        }


                                    } else {
                                        voteToHalt();
                                        break;
                                    }


                                }
                                break;
                            }

                        }

                        if (!containsId) {
                            voteToHalt();
                            return;
                        }

                    } else {
                        voteToHalt();
                        return;
                    }


                } catch (Exception e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }


            } else {
                long nodeId = Long.parseLong(parts[0]);
                long endTime = Long.parseLong(parts[1]);
                String licenceId = parts[2].trim();
                if(!subgraph.containsVertex(nodeId)) {
                    voteToHalt();
                    return;
                }
                try {

                    Iterable<? extends ISubgraphInstance> instances = subgraph.getInstances(endTime - 5 * 60 * 1000,
                            endTime, subgraph.getVertexProperties(), subgraph.getEdgeProperties(), false);

                    Iterator<? extends ISubgraphInstance> instanceIterator = instances.iterator();
                    if(instanceIterator.hasNext()) {
                        ISubgraphInstance instance = instanceIterator.next();

                        ISubgraphObjectProperties properties = instance.getPropertiesForVertex(nodeId);
                        String lids = (String) properties.getValue(LICENCE_LIST);
                        String lidParts [] = lids.split(",");

                        boolean found = false;

                        for(String l : lidParts) {
                            if(licenceId.equals(l)) {
                                found = true;
                                break;
                            }
                        }

                        if(found) {
                            logPath(nodeId,licenceId,endTime);
                            TemplateVertex lastKnownV = subgraph.getVertex(nodeId);
                            long startTime;
                            while (true) {
                                startTime = endTime;
                                endTime = startTime + 5*60*1000;
                                instances = subgraph.getInstances(startTime,endTime,
                                        subgraph.getVertexProperties(),subgraph.getEdgeProperties(),
                                        false);
                                instanceIterator = instances.iterator();
                                if(instanceIterator.hasNext()) {
                                    instance = instanceIterator.next();
                                    //Check in local sink nodes
                                    boolean foundInLocal = false;
                                    for(TemplateEdge edge : lastKnownV.outEdges()) {
                                        TemplateVertex sink = edge.getSink();
                                        if(sink.getId() == lastKnownV.getId()) {
                                            sink = edge.getSource();
                                        }

                                        if(sink != null && !subgraph.isRemoteVertex(sink.getId())) {
                                            ISubgraphObjectProperties props = instance.getPropertiesForVertex(sink.getId());
                                            String ids = (String)props.getValue(LICENCE_LIST);
                                            String[] idParts = ids.split(",");


                                            boolean contains = false;
                                            for (String id : idParts) {
                                                if (id.equals(licenceId)) {
                                                    contains = true;

                                                    break;
                                                }
                                            }

                                            if (contains) {
                                                foundInLocal = true;
                                                logPath(sink.getId(),licenceId,endTime);
                                                lastKnownV = sink;
//                                                    startTime = endTime;
//                                                    endTime = endTime + intervalDiff;
                                                break;
                                            }

                                        }
                                    }


                                    if(!foundInLocal) {
                                        for(TemplateEdge edge : lastKnownV.outEdges()) {
                                            TemplateVertex sink = edge.getSink();
                                            if(sink.getId() == lastKnownV.getId()) {
                                                sink = edge.getSource();
                                            }

                                            if(sink != null && subgraph.isRemoteVertex(sink.getId())) {
                                                String payLoad = "" + sink.getId() + "," + (endTime +5*60*1000)
                                                        + "," + licenceId;
                                                SubGraphMessage msg = new SubGraphMessage(payLoad.getBytes());
                                                msg.addTargetVertex(sink.getId());
                                                sentMessage(subgraph.getPartitionForRemoteVertex(sink.getId()),msg);
                                            }

                                        }

                                        voteToHalt();
                                        break;
                                    }


                                } else {
                                    voteToHalt();
                                    break;
                                }


                            }


                        } else {
                            voteToHalt();
                            return;
                        }







                    }  else {
                        voteToHalt();
                        return;
                    }

                } catch (Exception e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }


            }


        }
    }


    private void logPath(long nodeId, String lid, long endTime) {
        String msg = "" + (endTime - 5 * 60 * 1000) + "," + endTime + "," + nodeId + "," + lid;
        ;
        writer.println(msg);
        writer.flush();
    }
}
