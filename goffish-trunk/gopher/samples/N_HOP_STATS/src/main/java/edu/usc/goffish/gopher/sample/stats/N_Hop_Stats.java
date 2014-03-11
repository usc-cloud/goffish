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
package edu.usc.goffish.gopher.sample.stats;

import edu.usc.goffish.gofs.*;
import edu.usc.goffish.gopher.api.GopherSubGraph;
import edu.usc.goffish.gopher.api.SubGraphMessage;
import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;

import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class N_Hop_Stats extends GopherSubGraph {


    private ISubgraphInstance currentInstance;

    private Iterator<? extends ISubgraphInstance> instanceIterator;

    private int currentIteration;

    private boolean debugEnable = true;

  //  private static final String VANTAGE_IP_PROP = "vantage_ip";
    private static final String LATENCY_PROP = "latency";
    private static final String IS_EXIST_PROP = "is_exist";
    private static final String HOP_PROP = "hop";

    private Path logRootDir = Paths.get(".");

    private int hopCount = 6;

    @Override
    public void compute(List<SubGraphMessage> messageList) {

        if (getIteration() == 0 && getSuperStep() == 0) {
            String data = new String(messageList.get(0).getData());

         //   debugLog("GOT DATA initial :" + data);
            hopCount = Integer.parseInt(data);

            try {
                init();
            } catch (IOException e) {
                e.printStackTrace();
                throw new RuntimeException();
            }


        }
        long ls = System.currentTimeMillis();
        ISubgraphInstance instance = getCurrentInstance();



        if (instance == null) {
        //    debugLog("Instance == null : " + getIteration());
            voteToHalt();
            haultApp();
            return;
        }

        if (getSuperStep() == 0) {

            DescriptiveStatistics statistics = new DescriptiveStatistics();

            long diskTimeStart  = System.currentTimeMillis();
            if (!instance.hasProperties()) {
            //    debugLog("No Properties : " + getIteration());
                voteToHalt();
                return
                        ;
            }

            debugLog("INSTANCE_LOAD," + subgraph.getId() + "," +
                    (System.currentTimeMillis() - diskTimeStart) + "," + getSuperStep() + "," + getIteration());

            long travasalS = System.currentTimeMillis();
           // DescriptiveStatistics edgePropLoadTimeStats = new DescriptiveStatistics();
            for (ITemplateEdge edge : subgraph.edges()) {
             //   long edgePropStart = System.currentTimeMillis();
                ISubgraphObjectProperties edgeProps = instance.getPropertiesForEdge(edge.getId());
              //  edgePropLoadTimeStats.addValue(System.currentTimeMillis() - edgePropStart);

                String[] latencies = ((String) edgeProps.getValue(LATENCY_PROP)) == null ? null : ((String) edgeProps.getValue(LATENCY_PROP)).
                        split(",");
                String[] hops = ((String) edgeProps.getValue(HOP_PROP)) == null ? null : ((String) edgeProps.getValue(HOP_PROP)).split(",");


                if (hops != null && latencies != null) {

                    for (int i = 0; i < hops.length; i++) {
                        String h = hops[i];

                        if (hopCount == Integer.parseInt(h)) {
              //              debugLog("HOP : " + h + ": Latency : " + latencies[i]);
                            double latency = Double.parseDouble(latencies[i]);
                            statistics.addValue(latency);
                        }
                    }
                }


            }



            //debugLog("Travasal total : " + (System.currentTimeMillis() - travasalS));
            //debugLog("Edge Load Time max,avg:" + edgePropLoadTimeStats.getMax() + "," + edgePropLoadTimeStats.getMean());


            String data = "1:" + statistics.getMean();
            if (!"1:nan".equalsIgnoreCase(data)) {
                SubGraphMessage message = new SubGraphMessage(data.getBytes());
                sendMessage(partition.getId(), message);
                //debugLog("Sub-graph data sent : " + data);
            }
            voteToHalt();


        } else {

            if (acquireLock("N_HOP_" + partition.getId() + " _" + getIteration() + "_" + getSuperStep())) {
                //debugLog("Lock Acqured");
                DescriptiveStatistics statistics = new DescriptiveStatistics();
                boolean finalStage = false;
                for (SubGraphMessage msg : messageList) {

                    String data = new String(msg.getData());
                    //debugLog("Partittion got data : " + data);
                    String[] parts = data.split(":");
                    if ("1".equals(parts[0].trim())) {
                        if (!parts[1].equalsIgnoreCase("nan")) {
                            statistics.addValue(Double.parseDouble(parts[1]));
                            //debugLog("Stage 1 data added : " + parts[1]);
                        }
                    } else {
                        finalStage = true;
                        if (!parts[1].equalsIgnoreCase("nan")) {
                            statistics.addValue(Double.parseDouble(parts[1]));
                            //debugLog("Stage 2 data added : " + parts[1]);
                        }
                    }

                }


                if (finalStage) {
                    try {

                        String data = "" + statistics.getMean();
                        try{
                            Double.parseDouble(data);
                            sendMessageToReduceStep(new SubGraphMessage(data.getBytes()));
                        } catch (Exception e) {

                        }

                        PrintWriter writer = new PrintWriter(new FileWriter("Hop_Stats.log", true));
                        log(writer, hopCount, statistics.getMean(),
                                currentInstance.getTimestampStart());
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    voteToHalt();

                } else {

                    String data = "2:" + statistics.getMean();

                    if (!"2:nan".equalsIgnoreCase(data)) {
                        SubGraphMessage message = new SubGraphMessage(data.getBytes());
                        for (int i : partitions) {
                            sendMessage(i, message);
                        }
                        //debugLog("Stage 2 data sent :" + data);
                    }
                    voteToHalt();
                }
            } else {
                voteToHalt();
            }

        }


    }


    @Override
    public void reduce(List<SubGraphMessage> messageList) {

        if (getSuperStep() == 0) {

            if(messageList == null || messageList.isEmpty()) {
                voteToHalt();
                return;
            }

            for(SubGraphMessage msg : messageList) {

                SubGraphMessage m = new SubGraphMessage(msg.getData());

                for(int id : partitions) {
                    sendMessage(id,m);
                }
            }

        } else {

            DescriptiveStatistics statistics = new DescriptiveStatistics();
            for(SubGraphMessage message : messageList) {

                String data = new String(message.getData());
                Double d = Double.parseDouble(data);
                statistics.addValue(d);


            }

            PrintWriter writer = null;
            try {
                writer = new PrintWriter(new FileWriter("Hop_Stats.log", true));
                System.out.println("LOGGER STD_DIV: " + statistics.getStandardDeviation());
                writer.println("LOGGER STD_DIV: " + statistics.getStandardDeviation());
                writer.flush();
                writer.close();
            } catch (IOException e) {
                e.printStackTrace();
            }


        }

        System.out.println("[Gopher]Current Reduce Iteration : " + getIteration() +
                " Current SuperStep : " + getSuperStep());


    }



    private void init() throws IOException {
        //get current iteration
        currentIteration = getIteration();

        /**
         * Init the property filters
         */
        List<Property> properties = new ArrayList<>(1);
        properties.add(subgraph.getEdgeProperties().getProperty(LATENCY_PROP));
        properties.add(subgraph.getEdgeProperties().getProperty(IS_EXIST_PROP));
        properties.add(subgraph.getEdgeProperties().getProperty(HOP_PROP));
        //properties.add(subgraph.getEdgeProperties().getProperty(VANTAGE_IP_PROP));


        /**
         * Load the instance iterator from startTime to Long.MAX
         * Note that they will not get loaded in to the memory
         */
        instanceIterator = subgraph.getInstances(Long.MIN_VALUE,
                Long.MAX_VALUE, PropertySet.EmptyPropertySet, new PropertySet(properties), false).iterator();
        currentInstance = instanceIterator.hasNext() ? instanceIterator.next() : null;

    }


    private boolean nextStep() {
        return currentIteration == (getIteration() - 1);
    }


    private ISubgraphInstance getCurrentInstance() {

        if (nextStep()) {

            if (instanceIterator.hasNext()) {
                currentInstance = instanceIterator.next();
            } else {
                haultApp();




                currentInstance = null;
            }

            currentIteration = getIteration();

        }

        return currentInstance;


    }


    private boolean acquireLock(String lock) {
        boolean isAggregator;
        synchronized (N_Hop_Stats.class) {
            String fileName = "Lock_" + lock + ".lock";
            System.out.println("Lock " + fileName);


            Path filepath = logRootDir.resolve(fileName);
            try {
                Files.createFile(filepath);
                // if this succeeds, then this subgraph will perform agg for the partition
                isAggregator = true;
            } catch (FileAlreadyExistsException faeex) {
                isAggregator = false; // expected
            } catch (IOException e) {
                e.printStackTrace();
                isAggregator = false;
            }
        }
        System.out.println("Lock " + lock + " acqured by" + subgraph.getId());

        return isAggregator;

    }


    private void log(PrintWriter writer, int hop, double avg, long time) {
        System.out.println("LOGGER : " + hop + "," + avg);
        writer.println(hop + "," + avg + "," + time);
        writer.flush();
    }


    private void debugLog(String msg) {


        if(!debugEnable) {
            return;
        }


        PrintWriter writer;
        try {

            writer = new PrintWriter(new FileWriter("N_HOP_DEBUG" + partition.getId() + "_" +
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