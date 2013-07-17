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
package edu.usc.goffish.viz;

import ar.AggregateReducer;
import ar.Aggregates;
import ar.Glyphset;
import ar.Renderer;
import ar.ext.avro.AggregateSerailizer;
import ar.glyphsets.WrappedCollection;
import ar.glyphsets.implicitgeometry.Shaper;
import ar.glyphsets.implicitgeometry.Valuer;
import ar.renderers.ParallelSpatial;
import ar.rules.AggregateReducers;
import ar.rules.Aggregators;
import ar.rules.Transfers;
import ar.util.Util;
import edu.usc.goffish.gofs.*;
import edu.usc.goffish.gofs.namenode.FileNameNode;
import edu.usc.goffish.gofs.partition.gml.GMLFileIterable;
import edu.usc.goffish.gofs.partition.gml.GMLPartition;
import edu.usc.goffish.gofs.slice.FileStorageManager;
import edu.usc.goffish.gofs.slice.JavaSliceSerializer;
import edu.usc.goffish.gofs.slice.SliceManager;

import javax.ws.rs.core.UriBuilder;
import java.awt.*;
import java.awt.geom.AffineTransform;
import java.awt.geom.NoninvertibleTransformException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.Socket;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

public class SimpleAdjMatrix {

    private static final Logger logger = Logger.getLogger(SimpleAdjMatrix.class.getName());

    private static  String fileNameNode = null;
    private static  String slices = null;
    private static  String serverHost = "localhost";
    private static  int port = 8739;


    private static final String IS_EXIST_PROP = "is_exist";
    private Map<String, List> adjList = new HashMap<String, List>();

    private SliceManager sliceManager;

    private ExecutorService executor = Executors.newFixedThreadPool(10);
    public static void main(String[] args) throws Exception{
        int partitionId = 0;
        String graphId = null;
        if(args.length == 6) {
            fileNameNode = args[0];
            slices = args[1];
            serverHost = args[2];
            port = Integer.parseInt(args[3]);
            partitionId = Integer.parseInt(args[4]);
            graphId = args[5];

        } else {
            logger.severe("Invalid Arguments : arg[0] =  fileNameNodePath  arg[1]  = path to slices " +
                    "arg[2] = server host arg[3] = server port arg[4] = partition id " +
                    "arg[5] = graph id");
            throw new RuntimeException("Invalid Arguments");

        }

        SimpleAdjMatrix me = new SimpleAdjMatrix();
        logger.info("Initializing Partition data...");
        INameNode nameNode = me.getNameNode();
        IPartition partition = me.getPartition(nameNode,graphId,partitionId);

        logger.info("Creating adjcency list...");
        me.execute(partition);
        logger.info("Start Parallel Rendering processes...");
        me.parallelsDisplay();
    }



    private void parallelsDisplay() {

        List<String> dataList = new ArrayList<>();
        for (String key : adjList.keySet()) {
            List<String> list = adjList.get(key);

            for (String s : list) {

                dataList.add(key + ":" + s);

            }
        }


        logger.info("Finish processing the data set , size: " + dataList.size());

        int c = dataList.size()/500;
        for(int i = 0; i < 501;i++) {

            List<String> nDataList = new ArrayList<>();
            for(int k = 0; k < c;k++) {
                if((k + i*c) < (dataList.size() -1)) {
                    nDataList.add(dataList.get(k +i*c));
                }
            }
            logger.info("DataSet of size " + nDataList.size() + " submitted for paralled rendering");
            Exec exec = new Exec(nDataList);
            executor.execute(exec);
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }


    }

    public void execute(IPartition readPartition) throws IOException {


        for (ISubgraph sg : readPartition) {

            Iterable<ISubgraphInstance> instances = sliceManager.readInstances(sg, Long.MIN_VALUE,
                    Long.MAX_VALUE, sg.getVertexProperties(), sg.getEdgeProperties());

            ISubgraphInstance instance = instances.iterator().next();

            for (TemplateVertex vertex : instance.getTemplate().vertices()) {

                for (TemplateEdge edge : vertex.outEdges()) {
                    ISubgraphObjectProperties edgeProps = instance.getPropertiesForEdge(edge.getId());

                    Integer isExist = (Integer) edgeProps.getValue(IS_EXIST_PROP);
                    if (isExist == 1) {
                        Object w = "1";
                        String weight = w.toString();
                        weight += "#" + edge.getSink().getId();
                        String source = "" + edge.getSource().getId();
                        // System.out.println("Added w : " + weight + " for " + source);
                        if (adjList.get(source) == null) {
                            ArrayList<String> list = new ArrayList<String>();
                            list.add(weight);
                            adjList.put(source, list);
                        } else {
                            adjList.get(source).add(weight);
                        }
                    }

                }


            }

        }


    }


    private IPartition getPartition(INameNode nameNode,String graphId ,int partId)
            throws IOException {

        URI uri  = nameNode.getPartitionMapping(graphId,partId);

        Path tempPath = Paths.get(slices);
        sliceManager = new SliceManager(UUID.fromString(uri.getFragment()), new JavaSliceSerializer(),
                new FileStorageManager(tempPath));

        return sliceManager.readPartition();
    }


    private INameNode getNameNode() throws IOException {

        INameNode nameNode = new FileNameNode(Paths.get(fileNameNode));

        return nameNode;
    }

    public void send(Aggregates<?> aggs, String host, int port) throws Exception {
        Socket s = new Socket(host, port);
        AggregateSerailizer.serialize(aggs, s.getOutputStream());
        s.close();
        Thread.sleep(1000);
    }


    private class Exec implements Runnable {

        List<String> dataList = null;
        public Exec(List<String> dataList) {
            this.dataList = dataList;
        }

        @Override
        public void run() {
            Shaper shaper = new SimpleToRect(5, 5, 0, 1000);
            Valuer valuer = new Valuer.Constant();
            Glyphset glyphs = new WrappedCollection(dataList, shaper, valuer, Color.class);

            glyphs.bounds();

            Aggregators.Count aggregator = new Aggregators.Count();
            Transfers.Interpolate transfer = new Transfers.Interpolate(new Color(255, 0, 0, 25), new Color(255, 0, 0, 255));
            AggregateReducer reduction = new AggregateReducers.Count();

            Renderer render = new ParallelSpatial();

            int width = 600;
            int height = 600;
            AffineTransform vt = Util.zoomFit(glyphs.bounds(), width, height);
            try {
                vt.invert();
            } catch (NoninvertibleTransformException e) {
                e.printStackTrace();
            }

            logger.info("Start aggrigating data set..");
            Aggregates aggregates = render.reduce(glyphs, aggregator, vt, width, height);

            logger.info("Sending data " + aggregates);
            try {
                send(aggregates, serverHost, port);
            } catch (Exception e) {
                e.printStackTrace();
            }
            logger.info("Sending done " + aggregates);
        }
    }

}
