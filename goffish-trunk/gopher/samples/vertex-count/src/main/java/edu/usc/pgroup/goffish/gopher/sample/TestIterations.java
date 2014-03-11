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

import edu.usc.goffish.gopher.api.GopherSubGraph;
import edu.usc.goffish.gopher.api.SubGraphMessage;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;

public class TestIterations extends GopherSubGraph {

    @Override
    public void compute(List<SubGraphMessage> messageList) {

        if (getSuperStep() >= 4) {
            voteToHalt();


        }


        if (getSuperStep() == 1) {

            SubGraphMessage msg = new SubGraphMessage(("msg from : " + partition.getId() +
                    " : super step : " + getIteration() + " : " + getSuperStep()).getBytes());
            for (int i : partitions) {
                sendMessage(i, msg);
            }

        }

        if (getSuperStep() == 2) {
            for (SubGraphMessage m : messageList) {

                System.out.println("Got Message : " + new String(m.getData()));
                debugLog("Got Message : " + new String(m.getData()));
            }

        }
        if (getIteration() >= 5) {
            haultApp();
        }

        System.out.println("[Gopher]Current Iteration : " + getIteration() +
                " Current SuperStep : " + getSuperStep());


    }


    @Override
    public void reduce(List<SubGraphMessage> messageList) {

        if (getSuperStep() >= 4) {

            voteToHalt();
        }

        System.out.println("[Gopher]Current Reduce Iteration : " + getIteration() +
                " Current SuperStep : " + getSuperStep());


    }


    private void debugLog(String msg) {
        PrintWriter writer;
        try {

            writer = new PrintWriter(new FileWriter("Test_Debug" + partition.getId() + "_" +
                    subgraph.getId() + ".log", true));
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }

        writer.println(msg);
        writer.flush();
        writer.close();
    }
}
