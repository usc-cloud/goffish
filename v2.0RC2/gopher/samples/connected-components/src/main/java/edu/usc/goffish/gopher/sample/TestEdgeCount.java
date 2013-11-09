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

import edu.usc.goffish.gofs.ITemplateEdge;
import edu.usc.goffish.gopher.api.GopherSubGraph;
import edu.usc.goffish.gopher.api.SubGraphMessage;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;

public class TestEdgeCount extends GopherSubGraph {


    private PrintWriter writer;
    @Override
    public void compute(List<SubGraphMessage> messageList) {
        if(getSuperStep() == 0) {

            long local=0;
            long remote=0;

            for(ITemplateEdge edge : subgraph.edges()) {
               if(edge.getSink().isRemote()) {
                remote++;
               } else {
                   local++;
               }

            }


        }

        voteToHalt();
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
