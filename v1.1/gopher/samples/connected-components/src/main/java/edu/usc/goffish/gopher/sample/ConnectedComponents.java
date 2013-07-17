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

import edu.usc.goffish.gofs.TemplateEdge;
import edu.usc.goffish.gofs.TemplateVertex;
import edu.usc.goffish.gopher.api.GopherSubGraph;
import edu.usc.goffish.gopher.api.SubGraphMessage;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;


/**
 * See https://github.com/apache/giraph/blob/release-1.0/giraph-examples/src/main/java/org/apache/giraph/examples/ConnectedComponentsVertex.java
 */
public class ConnectedComponents extends GopherSubGraph {


    PrintWriter writer;

    private  long currentMin;

    private ArrayList<Long> rids = new ArrayList<>();

    @Override
    public void compute(List<SubGraphMessage> subGraphMessages) {


        if(getSuperStep() == 0) {
            rids = new ArrayList<>();

            long min = 0;
            for(TemplateVertex vertex : subgraph.vertices()) {
                if(min > vertex.getId()) {
                    min = vertex.getId();
                }

                if(vertex.isRemote()) {
                    rids.add(vertex.getId());
                }
            }

            currentMin = min;

            log(partition.getId(),subgraph.getId(),min);

            for(Long rv :rids) {
                int partId = subgraph.getPartitionForRemoteVertex(rv);
                String msg = "" + min;
                SubGraphMessage subGraphMessage = new SubGraphMessage(msg.getBytes());
                subGraphMessage.addTargetVertex(rv);
                sentMessage(partId,subGraphMessage);
            }


            voteToHalt();
            return;

        }


        boolean changed = false;


        for(SubGraphMessage msg : subGraphMessages) {

            long min = Long.parseLong(new String(msg.getData()));

            if(min < currentMin) {
                currentMin = min;
                changed = true;
            }


        }

        // propagate new component id to the neighbors

        if(changed) {

            log(partition.getId(),subgraph.getId(),currentMin);

            for(Long rv :rids) {
                int partId = subgraph.getPartitionForRemoteVertex(rv);
                String msg = "" + currentMin;
                SubGraphMessage subGraphMessage = new SubGraphMessage(msg.getBytes());
                subGraphMessage.addTargetVertex(rv);
                sentMessage(partId,subGraphMessage);
            }


        }

        voteToHalt();

    }


    private void log(int partId, long subId, long comId){
        if(writer == null) {
            try {
                writer = new PrintWriter(new FileOutputStream("CC_"+partId+"_"+subId+".txt",false));
            } catch (FileNotFoundException e) {
                e.printStackTrace();
                return;
            }
        }


        writer.print(partId + "," + subId + "," + comId);
        writer.flush();
    }


}
