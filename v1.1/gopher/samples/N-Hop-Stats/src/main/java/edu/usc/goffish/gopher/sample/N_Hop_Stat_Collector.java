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
import java.util.*;


public class N_Hop_Stat_Collector extends GopherSubGraph {


    private List<String> vantagePoints = new ArrayList<String>();

    private int N = 3;


    private static final String VANTAGE_IP_PROP = "vantage_ip";
    private static final String LATENCY_PROP = "latency";
    private static final String IS_EXIST_PROP = "is_exist";
    private static final String HOP_PROP = "hop";


    private Path logRootDir = Paths.get(".");

    @Override
    public void compute(List<SubGraphMessage> subGraphMessages) {

        /**
         * We do this in following steps.
         * Calculate stats for each subgraph.
         * Calculate aggregate stats for partition.
         * In this case a single sub-graph will do the aggregation
         * Aggregate partition level stats and combine at the smallest partition.
         */

        if (superStep == 0) {
            SubGraphMessage msg = subGraphMessages.get(0);
            String data = new String(msg.getData());

            String[] dataSplit = data.split("#");
            N = Integer.parseInt(dataSplit[0]);
            String[] vps = dataSplit[1].split(",");
            for (String vp : vps) {
                vantagePoints.add(vp.trim());
            }

            try {

                Iterable<? extends ISubgraphInstance> subgraphInstances =
                        subgraph.getInstances(Long.MIN_VALUE, Long.MAX_VALUE, PropertySet.EmptyPropertySet,
                                subgraph.getEdgeProperties(), false);

//                        sliceManager.readInstances(subgraph,
//                        Long.MIN_VALUE, Long.MAX_VALUE,
//                        PropertySet.EmptyPropertySet, subgraph.getEdgeProperties());

                for (ISubgraphInstance instance : subgraphInstances) {

                    Map<String, DescriptiveStatistics> statsMap =
                            new HashMap<String, DescriptiveStatistics>();


                    for (TemplateEdge edge : subgraph.edges()) {

                        ISubgraphObjectProperties edgeProps = instance.getPropertiesForEdge(edge.getId());

                        Integer isExist = (Integer) edgeProps.getValue(IS_EXIST_PROP);
                        if (isExist == 1) {
                            String[] vantageIps = ((String) edgeProps.getValue(VANTAGE_IP_PROP)).
                                    split(",");
                            String[] latencies = ((String) edgeProps.getValue(LATENCY_PROP)).
                                    split(",");
                            String[] hops = ((String) edgeProps.getValue(HOP_PROP)).split(",");

                            Integer[] vantangeIdx = vantageIpIndex(vantageIps);
                            if (vantangeIdx == null) {
                                continue;
                            }


                            for (int i : vantangeIdx) {

                                String vantage = vantageIps[i];
                                String latency = latencies[i];
                                String hop = hops[i];

                                double latency_num = Double.parseDouble(latency);
                                int hop_num = Integer.parseInt(hop);

                                if (latency_num >= 0 && hop_num == N) {
                                    if (statsMap.containsKey(vantage)) {

                                        statsMap.get(vantage).addValue(latency_num);

                                    } else {

                                        DescriptiveStatistics statistics =
                                                new DescriptiveStatistics();
                                        statistics.addValue(latency_num);
                                        statsMap.put(vantage, statistics);


                                    }
                                }
                                ;

                            }


                        }


                    }


                    int c = 0;
                    StringBuffer msgBuffer = new StringBuffer();

                    for (String v : statsMap.keySet()) {
                        c++;
                        DescriptiveStatistics statistics = statsMap.get(v);
                        String m = createMessageString(v, instance.getTimestampStart(),
                                instance.getTimestampEnd(), statistics.getStandardDeviation(),
                                statistics.getMean(), statistics.getN());

                        if (c == statsMap.keySet().size()) {
                            msgBuffer.append(m);
                        } else {

                            msgBuffer.append(m).append("|");
                        }

                    }

                    SubGraphMessage subMsg = new SubGraphMessage(msgBuffer.toString().getBytes());

                    sentMessage(partition.getId(), subMsg);


                }

            } catch (IOException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }


        } else if (superStep == 1) {
            //Ok here every sub-graph will receive message from its own partition.
            //Each message is belongs to a given some time span.
            Map<String, List<String[]>> vantageGroup = new HashMap<String, List<String[]>>();

            for (SubGraphMessage subGraphMessage : subGraphMessages) {

                String msgData = new String(subGraphMessage.getData());
                String[] dataParts = msgData.split("|");

                for (String data : dataParts) {
                    String[] vantageParts = data.split(",");
                    //Group by vantage point and startTime
                    if (vantageGroup.containsKey(vantageParts[0] + "|" + vantageParts[1])) {
                        vantageGroup.get(vantageParts[0] + "|" + vantageParts[1]).add(vantageParts);
                    } else {
                        ArrayList<String[]> arrayList = new ArrayList<String[]>();
                        arrayList.add(vantageParts);
                        vantageGroup.put(vantageParts[0] + "|" + vantageParts[1], arrayList);
                    }

                }

            }


            for (String key : vantageGroup.keySet()) {

                if (!acquireLock(key)) {
                    continue;
                }

                List<String[]> data = vantageGroup.get(key);

                double totalN = 0;
                double totalAvgVal = 0;

                double totalVar = 0;
                for (String[] d : data) {

                    //average
                    double mean = Double.parseDouble(d[4]);
                    long sN = Long.parseLong(d[5]);
                    totalN += sN;
                    totalAvgVal += mean * sN;


                    double sd = Double.parseDouble(d[3]);
                    totalVar += ((double) sd * sd) / ((double) sN);

                }

                double avg = totalAvgVal / totalN;
                double newSD = Math.sqrt(totalVar);

                //create message
                //sent to all the partitions except me.
                String msg = key + "," + newSD + "," + avg + "," + totalN;

                for (int pid : partitions) {
                    sentMessage(pid, new SubGraphMessage(msg.getBytes()));
                }


            }


        } else if (superStep >= 2) {

                if(partition.getId() == Collections.min(partitions)) {


                    Map<String,List<String[]>>  group = new HashMap<String,List<String[]>>();

                    for(SubGraphMessage msg : subGraphMessages) {

                        String data = new String(msg.getData());

                        String []dataParts = data.split(",");

                        if(group.containsKey(dataParts[0])) {
                                group.get(dataParts[0]).add(dataParts);
                        } else {
                            List<String[]> list = new ArrayList<String[]>();
                            list.add(dataParts);
                            group.put(dataParts[0],list);
                        }


                    }

                    if (!acquireLock("" + partition.getId())) {
                        voteToHalt();
                        return;
                    }

                    PrintWriter writer;
                    try {

                        writer = new PrintWriter(new FileWriter("TimeSeriesStats.csv"));
                    } catch (IOException e) {
                        e.printStackTrace();
                        throw new RuntimeException(e);
                    }
                    for(String key : group.keySet()) {


                        List<String[]> data = group.get(key);

                        double totalN = 0;
                        double totalAvgVal = 0;

                        double totalVar = 0;
                        for (String[] d : data) {

                            //average

                            //key + "," + newSD + "," + avg + "," + totalN;
                            double mean = Double.parseDouble(d[2]);
                            long sN = Long.parseLong(d[3]);
                            totalN += sN;
                            totalAvgVal += mean * sN;


                            double sd = Double.parseDouble(d[1]);
                            totalVar += ((double) sd * sd) / ((double) sN);

                        }

                        double avg = totalAvgVal / totalN;
                        double newSD = Math.sqrt(totalVar);

                        String vantage = key.split("|")[0];
                        String timeStamp = key.split("|")[1];

                        log(writer,vantage,timeStamp,avg,newSD);


                    }
                    writer.flush();
                    voteToHalt();

                }
        }

    }


    private String createMessageString(String vantage, long startTime, long endTime, double statderedDev, double average, long sampleSize) {
        StringBuffer buffer = new StringBuffer();
        return buffer.append(vantage).append(startTime).append(",").append(endTime).append(",").
                append(statderedDev).append(",").append(average).
                append(",").append(sampleSize).toString();
    }

    public Integer[] vantageIpIndex(String[] vantages) {
        if (vantages != null && vantages.length > 0) {

            ArrayList<Integer> indexes = new ArrayList<Integer>();

            for (int i = 0; i < vantages.length; i++) {
                if (vantagePoints.contains(vantages[i])) {
                    indexes.add(i);
                }
            }

            if (indexes.size() > 0) {
                return indexes.toArray(new Integer[indexes.size()]);
            } else {
                return null;
            }

        } else {
            return null;
        }

    }

    private boolean acquireLock(String lock) {

        String fileName = "Lock_" + lock + ".lock";

        boolean isAggregator;

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

        return isAggregator;

    }

    public void setVantagePoints(List<String> vantagePoints) {
        this.vantagePoints = vantagePoints;
    }

    public void setN(int n) {
        N = n;
    }


    private void log(PrintWriter writer, String vantage,String timeStamp, double avg, double sd) {
        writer.println(vantage + "," + timeStamp + "," + avg + "," + sd);
    }
}
