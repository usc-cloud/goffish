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

import edu.usc.goffish.gofs.ITemplateVertex;
import edu.usc.goffish.gopher.api.GopherSubGraph;
import edu.usc.goffish.gopher.api.SubGraphMessage;

import java.io.*;
import java.util.ArrayList;
import java.util.List;


/**
 * Implement the Subgraph centric version of the connected components algorithm.
 * The vertex centric BSP algorithm can be found at apache giraph examples
 * See https://github.com/apache/giraph/blob/release-1.0/giraph-examples/src/main/java/org/apache/giraph/examples/ConnectedComponentsVertex.java
 *
 * @author charith
 */
public class ConnectedComponents extends GopherSubGraph {


    private PrintWriter writer;

    private long currentMin;

    private ArrayList<Long> rids = new ArrayList<>();


    @Override
    public void compute(List<SubGraphMessage> subGraphMessages) {
        try {

            if (getSuperStep() == 0) {


                rids = new ArrayList<>();
                for (ITemplateVertex vertex : subgraph.remoteVertices()) {
                    rids.add(vertex.getId());
                }

                currentMin = subgraph.getId();

                log(partition.getId(), subgraph.getId(), currentMin);

                for (Long rv : rids) {
                    String msg = "" + currentMin;
                    SubGraphMessage subGraphMessage = new SubGraphMessage(msg.getBytes());
                    subGraphMessage.setTargetSubgraph(subgraph.getVertex(rv).getRemoteSubgraphId());
                    sendMessage(subGraphMessage);
                }

                voteToHalt();
                return;

            }

            boolean changed = false;

            for (SubGraphMessage msg : subGraphMessages) {

                long min = Long.parseLong(new String(msg.getData()));

                if (min < currentMin) {
                    currentMin = min;
                    changed = true;
                }


            }

            // propagate new component id to the neighbors

            if (changed) {

                log(partition.getId(), subgraph.getId(), currentMin);

                for (Long rv : rids) {

                    String msg = "" + currentMin;
                    SubGraphMessage subGraphMessage = new SubGraphMessage(msg.getBytes());
                    subGraphMessage.setTargetSubgraph(subgraph.getVertex(rv).getRemoteSubgraphId());
                    sendMessage(subGraphMessage);
                }


            }

            voteToHalt();
        } catch (Throwable e) {
            System.out.println(e);
            e.printStackTrace();
        }

    }

    private void log(int partId, long subId, long comId) {

        try {
            writer = new PrintWriter(new FileWriter("CC_" + partId + "_" + subId + ".txt", false));
        } catch (IOException e) {
            e.printStackTrace();
        }
        writer.print(partId + "," + subId + "," + comId + "," + subgraph.numVertices());
        writer.flush();
        writer.close();
    }


}
