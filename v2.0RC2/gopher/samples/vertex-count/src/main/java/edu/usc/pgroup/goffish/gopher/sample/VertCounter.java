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


import edu.usc.goffish.gofs.ITemplateVertex;
import edu.usc.goffish.gopher.api.GopherSubGraph;
import edu.usc.goffish.gopher.api.SubGraphMessage;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.List;

public class VertCounter extends GopherSubGraph {

    private int currentCount = 0;

    @Override
    public void compute(List<SubGraphMessage> subGraphMessages) {
        if(subGraphMessages == null || subGraphMessages.size() == 0) {

            currentCount = subgraph.getTemplate().numVertices();

            for(ITemplateVertex vertex : subgraph.remoteVertices()) {
                currentCount--;
            }

            for(long part : partitions) {
                String count = "" + partition.getId() + ":" + subgraph.getId() + ":" + currentCount;
                SubGraphMessage msg = new SubGraphMessage(count.getBytes());
                sendMessage(part,msg);
            }

        } else {

            for(SubGraphMessage msg : subGraphMessages) {

                String count = new String(msg.getData());
                String dataParts[] = count.split(":");

                long partId = Long.parseLong(dataParts[0]);
                long subId = Long.parseLong(dataParts[1]);

                if(!(partition.getId() == partId && subgraph.getId() == subId)) {
                    currentCount += Integer.parseInt(dataParts[2]);
                }

            }


            try {
                File file = new File("vert-count.txt");
                PrintWriter writer = new PrintWriter(file);
                writer.write("Total Vertex Count :" + currentCount );
                writer.flush();
                writer.close();
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
            voteToHalt();
        }
    }
}
