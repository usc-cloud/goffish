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
package edu.usc.goffish.gopher.sample;

import edu.usc.goffish.gofs.*;
import edu.usc.goffish.gofs.partition.TemplateVertex;
import edu.usc.goffish.gofs.util.DisjointSets;
import edu.usc.goffish.gopher.api.GopherSubGraph;
import edu.usc.goffish.gopher.api.SubGraphMessage;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;

public class ConnectedComponentsOverTimeSeries extends GopherSubGraph {


    private static final String IS_EXIST = "is_exist";

    private long lastEnd;

    private PrintWriter writer;


    private Collection<? extends Collection<ITemplateVertex>> realSubs;

    private Map<Collection<ITemplateVertex>, Long> subToMinMap = new HashMap<>();

    private int oldIteration;

    @Override
    public void compute(List<SubGraphMessage> subGraphMessages) {

        if (getSuperStep() == 0 && getIteration() == 0) {
            try {
                lastEnd = getFirst().getTimestampStart();
            } catch (IOException e) {
                e.printStackTrace();
                return;
            }
        }

        if (oldIteration != getIteration()) {
            lastEnd += 24 * 60 * 60;
            writer = null;
            oldIteration = getIteration();
        }

        if (getSuperStep() == 0) {

            realSubs = findSubgraphs();


            for (Collection<ITemplateVertex> sub : realSubs) {
                ArrayList<Long> rids = new ArrayList<>();

                long min = 0;
                long currentMin = 0;
                for (ITemplateVertex vertex : sub) {
                    if (min > vertex.getId()) {
                        min = vertex.getId();
                    }

                    if (vertex.isRemote()) {
                        rids.add(vertex.getId());
                    }
                }

                currentMin = min;

                subToMinMap.put(sub, currentMin);

                log(partition.getId(), subgraph.getId(), sub.toString(), min, sub.size());

                for (Long rv : rids) {

                    String msg = "" + min + ":" + rv;
                    SubGraphMessage subGraphMessage = new SubGraphMessage(msg.getBytes());
                    subGraphMessage.setTargetSubgraph(subgraph.getVertex(rv).getRemoteSubgraphId());
                    sendMessage(subGraphMessage);
                }


            }

            voteToHalt();
            return;

        }


        for (SubGraphMessage message : subGraphMessages) {

            String[] data = new String(message.getData()).split(":");
            long min = Long.parseLong(data[0]);
            long rid = Long.parseLong(data[1]);

            Collection<ITemplateVertex> sub = getSubgraph(rid);
            if (subToMinMap.get(sub) > min) {
                subToMinMap.put(sub, min);
                log(partition.getId(), subgraph.getId(), sub.toString(), min, sub.size());
                ArrayList<Long> rids = new ArrayList<>();

                for (ITemplateVertex vertex : sub) {

                    if (vertex.isRemote()) {
                        rids.add(vertex.getId());
                    }
                }

                for (Long rv : rids) {
                    String msg = "" + min + ":" + rv;
                    SubGraphMessage subGraphMessage = new SubGraphMessage(msg.getBytes());
                    subGraphMessage.setTargetSubgraph(subgraph.getVertex(rv).getRemoteSubgraphId());
                    sendMessage(subGraphMessage);
                }
            }

        }

        voteToHalt();


    }


    private void log(int partId, long subId, String sub, long comId, long count) {

        if (writer == null) {
            try {
                writer = new PrintWriter(new FileOutputStream("CC_" + partId + "_" + subId + "_" +
                        getIteration() + "_" + sub + ".txt", false));
            } catch (FileNotFoundException e) {
                e.printStackTrace();
                return;
            }
        }


        writer.print(partId + "," + subId + "," + comId + "," + count);
        writer.flush();
    }


    public Collection<? extends Collection<ITemplateVertex>> findSubgraphs() {

        DisjointSets<ITemplateVertex> ds = new DisjointSets<ITemplateVertex>(subgraph.numVertices());

        for (ITemplateVertex vertex : subgraph.vertices()) {
            ds.addSet(vertex);
        }


        for (ITemplateEdge edge : subgraph.edges()) {
            if (isExistEdge(edge)) {
                ITemplateVertex source = edge.getSource();
                ITemplateVertex sink = edge.getSink();
                ds.union(source, sink);
            }
        }

        return ds.retrieveSets();

    }


    private boolean isExistEdge(ITemplateEdge edge) {


        List<Property> properties = new ArrayList<>();
        properties.add(subgraph.getVertexProperties().getProperty("is_exist"));

        try {
            Iterable<? extends ISubgraphInstance> instances = subgraph.getInstances(lastEnd, lastEnd + 24 * 60 * 60, PropertySet.EmptyPropertySet, new PropertySet(properties), false);

            for (ISubgraphInstance instance : instances) {

                ISubgraphObjectProperties props = instance.getPropertiesForEdge(edge.getId());
                Boolean bool = (Boolean) props.getValue(IS_EXIST);
                if (bool) {
                    return true;
                }
            }


        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;

    }


    private Collection<ITemplateVertex> getSubgraph(long id) {

        ITemplateVertex vertex = subgraph.getVertex(id);

        for (Collection<ITemplateVertex> sub : realSubs) {
            if (sub.contains(vertex)) {
                return sub;
            }
        }

        return null;
    }


    public ISubgraphInstance getFirst() throws IOException {

        return subgraph.getInstances(Long.MIN_VALUE, Long.MAX_VALUE, subgraph.getVertexProperties(),
                subgraph.getEdgeProperties(), false).iterator().next();

    }


}
