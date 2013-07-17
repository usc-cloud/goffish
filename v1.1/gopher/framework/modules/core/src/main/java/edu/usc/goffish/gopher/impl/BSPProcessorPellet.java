/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *   * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package edu.usc.goffish.gopher.impl;

import edu.usc.goffish.gofs.INameNode;
import edu.usc.goffish.gofs.IPartition;
import edu.usc.goffish.gofs.ISubgraph;
import edu.usc.goffish.gofs.namenode.FileNameNode;
import edu.usc.goffish.gofs.slice.FileStorageManager;
import edu.usc.goffish.gofs.slice.JavaSliceSerializer;
import edu.usc.goffish.gofs.slice.SliceManager;
import edu.usc.goffish.gopher.api.GopherSubGraph;
import edu.usc.goffish.gopher.api.SubGraphMessage;
import edu.usc.goffish.gopher.bsp.BSPMessage;
import edu.usc.pgroup.floe.api.exception.LandmarkException;
import edu.usc.pgroup.floe.api.exception.LandmarkPauseException;
import edu.usc.pgroup.floe.api.framework.pelletmodels.BSPPellet;
import edu.usc.pgroup.floe.api.stream.FIterator;
import edu.usc.pgroup.floe.api.stream.FMapEmitter;
import edu.usc.pgroup.floe.api.util.BitConverter;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Semaphore;
import java.util.logging.Logger;

/**
 * <class>BSPProcessorPellet</class> is a internal class which implements the BSP behaviour using flow.
 * This represents a processor in BSP system.
 */
public class BSPProcessorPellet implements BSPPellet {

    private List<String> partitionIds = new ArrayList<String>();


    // we need to preserve insertion ordering to ensure coscheduling of similar subgraphs from
    // partition iterator
    //private SortedMap<Integer, ISubgraph> subGraphMap = new TreeMap<Integer, ISubgraph>();
    private Map<Integer, ISubgraph> subGraphMap = new LinkedHashMap<Integer, ISubgraph>();
    
    private Map<Long, GopherSubGraph> subGraphProcessorMap = new HashMap<Long, GopherSubGraph>();

    private List<Integer> partList;

    private String sliceFilePath = "slices";

    private IPartition partition;

    private SliceManager sliceManager;

    private int currentSuperStep;


    private ForkJoinPool pool;
    private static Logger logger = Logger.getLogger(BSPProcessorPellet.class.getName());

    private static final String configFilePath ="bsp-config"+ File.separator +"gopher.properties";

    private Properties properties;


    private static final String clzz = "class";
    private static final String slicePath = "slicePath";
    private static final String graphId = "graphId";
        private static final String partitionId = "partitionId";
    private static final String nameNodeFilePath = "nameNodeFile";




    private Class implClass;

    /**
     * Consts
     */
    private String controlKey = "CONTROL";


    public BSPProcessorPellet() {

        System.out.println("****************BSP Processor*****************");

        // Creating slice manager
        try {

            properties = new Properties();
            properties.load(new FileInputStream(configFilePath));
            String impl = properties.getProperty(clzz);
            implClass = Class.forName(impl);

            if(properties.containsKey(slicePath)) {
                sliceFilePath = properties.getProperty(slicePath);
            }

            String nameNodeFile = properties.getProperty(nameNodeFilePath);
            String gid = properties.getProperty(graphId);

            INameNode nameNode = new FileNameNode(Paths.get(nameNodeFile));

            partList  = new ArrayList<Integer>();
            partList.addAll(nameNode.getPartitions(gid));

//            String managerHost = ContainerEnvironment.getEnvironment().
//                    getSystemConfigParam(Constants.MANAGER_HOST);
//            int managerPort =  Integer.parseInt(ContainerEnvironment.getEnvironment().
//                    getSystemConfigParam(Constants.MANAGER_PORT));
//            Socket s= null;
//            try{
//                s = new Socket(managerHost,managerPort);
//            }catch (IOException e ) {
//                logger.severe("Can't connect to manager check ContainerConfiguration or " +
//                        "Contact cluster admistrator clause--" + e.getCause());
//                throw new IOError(e);
//            }
//
//            String currentHost = s.getInetAddress().getHostAddress().trim();
//            s.close();


            URI currentPartURI = null;

            // FIXME: is there a more deterministic way of getting the "current" partition URI?
            if(properties.containsKey(partitionId)) {
                currentPartURI = nameNode.getPartitionMapping(gid,Integer.
                        parseInt(properties.getProperty(partitionId)));
            } else {
                for (int pid : partList) {
                    URI uri = nameNode.getPartitionMapping(gid, pid);
                    if (uri.getPath().contains(sliceFilePath) || sliceFilePath.contains(uri.getPath())) {
                        currentPartURI = uri;
                    }
                }
            }


            Path tempPath = Paths.get(sliceFilePath);
            sliceManager = new SliceManager(UUID.fromString(currentPartURI.getFragment()), new JavaSliceSerializer(),
                    new FileStorageManager(tempPath));


            partition = sliceManager.readPartition();

            // since we're using a linkedhashmap, the order in which we insert into the map 
            // is the order in which the keys are returned (used when scheduling subgraphs)
            for(ISubgraph subgraph : partition) {

                subGraphMap.put((int)subgraph.getId(),subgraph);
            }

            // allocate twice the number of threads as the number of processors-1, but atleast one thread!
            int concurrentSubGraphSlots = (Runtime.getRuntime().availableProcessors() - 1) * 2;
            concurrentSubGraphSlots = concurrentSubGraphSlots <= 0? 1 : concurrentSubGraphSlots;
            pool = new ForkJoinPool(concurrentSubGraphSlots); 

        } catch (IOException e) {
            logger.severe("Error while Initializing Gopher " + e);
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            logger.severe("Error while Initializing Gopher " + e);
            e.printStackTrace();
        }

    }


    public void invoke(FIterator fIterator, FMapEmitter fMapEmitter) {
        String currentKey = "";
        Map<Long, List<SubGraphMessage>> messageGroups = new HashMap<Long, List<SubGraphMessage>>();

        while (true) {

            try {
                Object o = fIterator.next();

                if (o instanceof BSPMessage) {
                    BSPMessage message = (BSPMessage) o;
                    
                    // Handle a control message that arrived from manager
                    if (message.getType() == BSPMessage.CTRL) {

	                    currentSuperStep = message.getSuperStep();
	                    // since we're using a linkedhashmap, the order in which we insert into the map 
	                    // is the order in which the keys are returned (used when scheduling subgraphs)
	                    Iterator<Integer> subgraphIter = subGraphMap.keySet().iterator();
	
	                    // tells us when all subtasks are done
                        Semaphore semaphore = new Semaphore(subGraphMap.keySet().size());
                        semaphore.acquire(subGraphMap.keySet().size());
                        int scheduled = 0;
                        while (subgraphIter.hasNext()) {
                        	// scheduling oder is the iterator order. Good!
                            ISubgraph subgraph = subGraphMap.get(subgraphIter.next()); 

                            // do we have a prior processor for this subgraph ID 
                            // (e.g. from previous superstep)? If so, reuse.
                            GopherSubGraph processor = null;
                            if (subGraphProcessorMap.containsKey(subgraph.getId())) {
                                processor = subGraphProcessorMap.get(subgraph.getId());

                            } else {
                                processor = newSubgraphProcessorImpl();
                                subGraphProcessorMap.put(subgraph.getId(),processor);
                            }

                            SubGraphTaskRunner task = new SubGraphTaskRunner(partition, subgraph,processor,
                            		fMapEmitter, semaphore,messageGroups.get(subgraph.getId()));
                            pool.execute(task);
                            scheduled++;
                        }

                        // wait for all scheduled tasks to complete
                        semaphore.acquire(scheduled);

                        // create sync message for manager
                        BSPMessage bspMessage = new BSPMessage();
                        bspMessage.setSuperStep(currentSuperStep);
                        bspMessage.setType(BSPMessage.CTRL);
                        bspMessage.setKey(controlKey);

                        // do all subgraphs want to halt?
                        boolean halt  = true;
                        for(GopherSubGraph processor : subGraphProcessorMap.values()) {
                            if(!processor.isVoteToHalt()) {
                                halt = false;
                                break;
                            }
                        }

                        if (halt) {
                            bspMessage.setVoteToHalt(true);
                        } else {
                            bspMessage.setVoteToHalt(false);
                        }

                        // send sync message to manager
                        messageGroups.clear();
                        fMapEmitter.emit(controlKey, bspMessage);

                    } else {  // Handle a data message that arrived from workers

                        currentKey = message.getKey();
                        if(currentKey == null) {
                            currentKey = "" + partition.getId();
                        }
                        String keyParts[] = currentKey.split(":"); // fixme: this is risky!
                        if (keyParts.length == 2) { 
                        	// we know which subgraph in partition to route message to
                        	//String partitionId = currentKey.split(":")[0];
                            String vertId = currentKey.split(":")[1];
                            long vid = Long.parseLong(vertId);
                            long sid = partition.getSubgraphForVertex(vid).getId();
                            ISubgraph subGraph = partition.getSubgraph(sid);
                            if (!subGraphMap.containsValue(subGraph)) {
                                subGraphMap.put(subGraph.getTemplate().numVertices(), subGraph);
                            }

                            SubGraphMessage msg = (SubGraphMessage) readObject(message.getData());

                            if (messageGroups.containsKey(subGraph.getId())) {
                                messageGroups.get(subGraph.getId()).add(msg);
                            } else {
                                ArrayList<SubGraphMessage> mList = new ArrayList<SubGraphMessage>();
                                mList.add(msg);
                                messageGroups.put(subGraph.getId(), mList);
                            }
                        } else  {
                        	// we don't know which subgraph in partition to route message to.
                            // FIXME: So broadcast the message to all subgraphs in this partition                        	
                            SubGraphMessage msg = (SubGraphMessage) readObject(message.getData());
                            for(ISubgraph subGraph : partition) {

                                if (!subGraphMap.containsValue(subGraph)) {
                                    subGraphMap.put(subGraph.getTemplate().numVertices(), subGraph);
                                }

                                if (messageGroups.containsKey(subGraph.getId())) {
                                    messageGroups.get(subGraph.getId()).add(msg);
                                } else {
                                    ArrayList<SubGraphMessage> mList = new ArrayList<SubGraphMessage>();
                                    mList.add(msg);
                                    messageGroups.put(subGraph.getId(), mList);
                                }

                            }

                        }
                    }
                }
                else { // o not BSPMessage
                	logger.severe("Unknown message type (not BSP message}:" + o);
                }
                

            } catch (LandmarkException e) {
                e.printStackTrace();
            } catch (LandmarkPauseException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
            }

        }
    }



    private Object readObject(byte[] data) {
        return BitConverter.getObject(data);
    }

    private GopherSubGraph newSubgraphProcessorImpl() throws Exception {
        GopherSubGraph instance = (GopherSubGraph) implClass.newInstance();
        return instance;
    }


    private class SubGraphTaskRunner implements Runnable {

        private IPartition partition;
        private ISubgraph subgraph;
        private GopherSubGraph processor;
        private List<SubGraphMessage> messages;

        private FMapEmitter emitter;
        private Semaphore semaphore;

        private SubGraphTaskRunner(IPartition partition, ISubgraph subgraph, GopherSubGraph processor,
                                FMapEmitter emitter, Semaphore semaphore, List<SubGraphMessage> messages) {
            this.partition = partition;
            this.subgraph = subgraph;
            this.processor = processor;
            this.emitter = emitter;
            this.semaphore = semaphore;
            this.messages = messages;
        }

        @Override
        public void run() {
        	
        	// initialize processor
            Map<Long, List<SubGraphMessage>> outBuffer = new HashMap<Long, List<SubGraphMessage>>();
            if (processor.isCleanedUp()) {
                processor.init(partition, subgraph, sliceManager,partList,outBuffer);
            }
            processor.setSuperStep(currentSuperStep);
            
            // call compute, as long as it did not halt previously and it has no new messages
            if(!(processor.isVoteToHalt() && messages.isEmpty())) {
                processor.compute(messages); // FIXME: is this right, according to the pregel model?
            }
            
            // Emit output messages to remote workers
            //TODO pipeLineThis by adding emiting
            if (!outBuffer.isEmpty()) {
                Iterator<Long> longIt = outBuffer.keySet().iterator();
                while (longIt.hasNext()) {
                    long partId = longIt.next();

                    for (SubGraphMessage m : outBuffer.get(partId)) {
                        byte[] data = BitConverter.getBytes(m);
                        BSPMessage bspMessage = new BSPMessage();
                        bspMessage.setSuperStep(currentSuperStep);
                        bspMessage.setType(BSPMessage.DATA);
                        bspMessage.setKey("" + partId);
                        bspMessage.setData(data);
                        if(m.getTargetVertex() == 0) {
                            emitter.emit("" + partId, bspMessage);
                        } else {
                            emitter.emit("" + partId + ":" + m.getTargetVertex(), bspMessage);
                        }

                    }
                }
            }

            // signal to BSPProcessor that this task is completed
            semaphore.release();
        }
    }

}
