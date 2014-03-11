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
package edu.usc.goffish.gopher.sample.lpr;

import edu.usc.goffish.gofs.*;
import edu.usc.goffish.gopher.api.GopherSubGraph;
import edu.usc.goffish.gopher.api.SubGraphMessage;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;

public class CarTracer extends GopherSubGraph {

    private int currentIteration;

    private Iterator<? extends ISubgraphInstance> instanceIterator;

    private ISubgraphInstance currentInstance;

    private static PropertySet LICENSE_PLACE;

    private static PropertySet DISTANCE;

    private final static String LICENSE_PLACE_ATT = "license_plate";

    private final static String DISTANCE_ATT = "distance";

    private static int maxSpeed = 70;

    private static float maxDistance;

    private static long timeDuation;

    private String licenceId;

    private Map<Integer, Long> lastKnownLocation = new HashMap<>();


    @Override
    public void compute(List<SubGraphMessage> messageList) {

        if (getIteration() == 0 && getSuperStep() == 0) {

            String data = new String(messageList.get(0).getData());
            String[] parts = data.split(":");
            try {
                init(Long.parseLong(parts[0]));
            } catch (IOException e) {
                e.printStackTrace();
            }

            licenceId = parts[1];
            long vertexId = findCar(parts[1]);

            //Car is not in this sub-graph
            if (vertexId == -1) {
                voteToHalt();
                return;
            }
            logLocation("" + vertexId,licenceId,Long.parseLong(parts[0]));
            List<DFSVertex> inList = new ArrayList<>();
            DFSVertex inV = new DFSVertex();
            inV.vertex = vertexId;
            inV.currentDistance = 0;
            inList.add(inV);
            List<DFSVertex> remoteList = DFS(inList, maxSpeed, licenceId);

            if (remoteList != null && !remoteList.isEmpty()) {
                sendToRemote(remoteList);
            }

            voteToHalt();

        } else if (getIteration() == 0) {
            getCurrentInstance();
            List<DFSVertex> inList = new ArrayList<>(messageList.size());
            for (SubGraphMessage message : messageList) {
                //vid,currentDistance
                String data = new String(message.getData());
                String parts[] = data.split(",");

                DFSVertex inV = new DFSVertex();
                inV.vertex = Long.parseLong(parts[0]);
                inV.currentDistance = Float.parseFloat(parts[1]);
                inList.add(inV);
            }

            List<DFSVertex> remoteList = DFS(inList, maxSpeed, licenceId);

            if (remoteList != null && !remoteList.isEmpty()) {
                sendToRemote(remoteList);
            }

            voteToHalt();


        } else {
            getCurrentInstance();
            if (getSuperStep() == 0) {

                //TODO : check we need to go beyond one iteration
                if (lastKnownLocation.containsKey(getIteration() - 1)) {
                    long lastKnownVertex = lastKnownLocation.get(getIteration() - 1);

                    List<DFSVertex> inList = new ArrayList<>();
                    DFSVertex inV = new DFSVertex();
                    inV.vertex = lastKnownVertex;
                    inV.currentDistance = 0;
                    inList.add(inV);
                    List<DFSVertex> remoteList = DFS(inList, maxSpeed, licenceId);

                    if (remoteList != null && !remoteList.isEmpty()) {
                        sendToRemote(remoteList);
                    }

                    voteToHalt();

                }


            } else {
               getCurrentInstance();
                List<DFSVertex> inList = new ArrayList<>(messageList.size());
                for (SubGraphMessage message : messageList) {
                    //vid,currentDistance
                    String data = new String(message.getData());
                    String parts[] = data.split(",");

                    DFSVertex inV = new DFSVertex();
                    inV.vertex = Long.parseLong(parts[0]);
                    inV.currentDistance = Float.parseFloat(parts[1]);
                    inList.add(inV);
                }

                List<DFSVertex> remoteList = DFS(inList, maxSpeed, licenceId);

                if (remoteList != null && !remoteList.isEmpty()) {
                    sendToRemote(remoteList);
                }

                voteToHalt();
            }
        }


    }


    private void sendToRemote(List<DFSVertex> remoteList) {
        /**
         * message format
         * vid,currentDistance
         */

        for (DFSVertex vertex : remoteList) {

            String data = vertex.vertex + "," + vertex.currentDistance;
            SubGraphMessage message = new SubGraphMessage(data.getBytes());
            message.setTargetSubgraph(subgraph.getVertex(vertex.vertex).getRemoteSubgraphId());
            sendMessage(message);

        }
    }

    /**
     * A non-recursive implementation of DFS:
     * http://en.wikipedia.org/wiki/Depth-first_search
     *
     * @param vertices    source vertices and its starting depth
     * @param maxDistance max distance to search
     * @return Return list of remote vertices and its
     */
    private List<DFSVertex> DFS(List<DFSVertex> vertices, int maxDistance, String licenceId) {

        List<DFSVertex> remoteVertexList = new ArrayList<>();
        List<Long> visited = new ArrayList<>(subgraph.numVertices());

        for (DFSVertex vertex : vertices) {
            if (visited.contains(vertex.vertex)) {
                continue;
            }


            Deque<DFSVertex> stack = new ArrayDeque<>();
            stack.push(vertex);

            parent:
            while (!stack.isEmpty()) {
                DFSVertex top = stack.peek();
                if (subgraph.getVertex(top.vertex).isRemote()) {
                    //add remote
                    if (top.currentDistance < maxDistance)
                        remoteVertexList.add(top);
                }

                String attVal = (String) getCurrentInstance().getPropertiesForVertex(top.vertex).
                        getValue(LICENSE_PLACE_ATT);
                visited.add(top.vertex);
                if (licenceId.equals(attVal)) {
                    lastKnownLocation.put(getIteration(), top.vertex);
                    logLocation("" + top.vertex, licenceId, getCurrentInstance().getTimestampStart());
                    if (vertex.currentDistance >= maxDistance) {
                        return remoteVertexList;
                    }
                }

                for (ITemplateEdge e : subgraph.getVertex(top.vertex).outEdges()) {

                    ITemplateVertex sink = e.getSink(subgraph.getVertex(top.vertex));
                    Float distance = (Float) getCurrentInstance().getPropertiesForEdge(e.getId()).
                            getValue(DISTANCE_ATT);

                    DFSVertex w = new DFSVertex();
                    w.currentDistance = top.currentDistance + distance;
                    w.vertex = sink.getId();

                    if (visited.contains(sink.getId())) {
                        continue;
                    }

                    if (w.currentDistance < maxDistance) {
                        stack.push(w);
                    }

                    continue parent;

                }
                stack.pop();
            }



        }

        return remoteVertexList;
    }

    /**
     * Find the car from the
     *
     * @param licenceId
     * @return
     */
    private long findCar(String licenceId) {
        for (ITemplateVertex vertex : subgraph.vertices()) {
            ISubgraphInstance instance = getCurrentInstance();
            if(instance == null) {
                return -1;
            }
            ISubgraphObjectProperties props = getCurrentInstance().getPropertiesForVertex(vertex.getId());
            String licence = (String) props.getValue(LICENSE_PLACE_ATT);

            if (licenceId.equals(licenceId)) {
                return vertex.getId();
            }
        }

        return -1;
    }


    private void init(long startTime) throws IOException {
        //get current iteration
        currentIteration = getIteration();

        /**
         * Init the property filters
         */
        List<Property> properties = new ArrayList<>(1);
        properties.add(subgraph.getVertexProperties().getProperty("license_plate"));  //string
        LICENSE_PLACE = new PropertySet(properties);
        properties = new ArrayList<>();
        properties.add(subgraph.getEdgeProperties().getProperty("distance"));  //float
        DISTANCE = new PropertySet(properties);


        /**
         * Load the instance iterator from startTime to Long.MAX
         * Note that they will not get loaded in to the memory
         */
        instanceIterator = subgraph.getInstances(startTime,
                Long.MAX_VALUE, LICENSE_PLACE, DISTANCE, false).iterator();
        currentInstance = instanceIterator.hasNext() ? instanceIterator.next():null;
    }


    private boolean nextStep() {
        return currentIteration == (getIteration() - 1);
    }

    private ISubgraphInstance getCurrentInstance() {

        if (nextStep()) {

            if (instanceIterator.hasNext()) {

                currentInstance = instanceIterator.next();
                long timeDiff = currentInstance.getTimestampEnd() -
                        currentInstance.getTimestampStart();
                timeDuation = timeDiff;
                maxDistance = ((float) timeDiff * maxSpeed) / ((float) 1000 * 60 * 60);
            } else {
                haultApp();
                currentInstance = null;
            }

            currentIteration = getIteration();

        }

        return currentInstance;


    }


    private class DFSVertex {

        public long vertex;

        public float currentDistance;

    }

    private void logLocation(String vertexId, String licenceId, long time) {
        PrintWriter writer;
        try {

            writer = new PrintWriter(new FileWriter("LPR_" + partition.getId() + "_" +
                    subgraph.getId() + ".txt", false));
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }

        writer.print(partition.getId() + "," + subgraph.getId() + "," + vertexId + "," +
                licenceId + "," + time);
        writer.flush();
        writer.close();
    }



}
