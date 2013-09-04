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
package edu.usc.goffish.gopher.impl;

import edu.usc.goffish.gofs.*;
import edu.usc.goffish.gofs.namenode.DataNode;
import edu.usc.goffish.gofs.namenode.RemoteNameNode;
import edu.usc.goffish.gofs.util.URIHelper;
import edu.usc.goffish.gopher.api.GopherSubGraph;
import edu.usc.goffish.gopher.api.SubGraphMessage;
import edu.usc.goffish.gopher.bsp.BSPMessage;
import edu.usc.goffish.gopher.impl.util.StatLogger;
import edu.usc.pgroup.floe.api.exception.LandmarkException;
import edu.usc.pgroup.floe.api.exception.LandmarkPauseException;
import edu.usc.pgroup.floe.api.framework.pelletmodels.BSPPellet;
import edu.usc.pgroup.floe.api.stream.FIterator;
import edu.usc.pgroup.floe.api.stream.FMapEmitter;
import edu.usc.pgroup.floe.api.util.BitConverter;
import it.unimi.dsi.fastutil.ints.IntCollection;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.*;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Semaphore;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * <class>BSPProcessorPellet</class> is a internal class which implements the BSP behaviour using flow.
 * This represents a processor in BSP system.
 */
public class BSPProcessorPellet implements BSPPellet {


    private static final String JAR_DIR = "apps";


    // we need to preserve insertion ordering to ensure coscheduling of similar subgraphs from
    // partition iterator
    //private SortedMap<Integer, ISubgraph> subGraphMap = new TreeMap<Integer, ISubgraph>();
    private Map<Long, ISubgraph> subGraphMap = new LinkedHashMap<Long, ISubgraph>();

    private Map<Long, Integer> subgraphToPartitionMap = new HashMap<>();

    private Map<Long, GopherSubGraph> subGraphProcessorMap = new HashMap<Long, GopherSubGraph>();

    private List<Integer> partList = new ArrayList<>();

    private IPartition partition;

    private IDataNode dataNode;

    private int currentSuperStep;

    private int currentIteration;

    private ForkJoinPool pool;

    private static Logger logger = Logger.getLogger(BSPProcessorPellet.class.getName());


    private Class implClass;

    /**
     * Consts
     */
    private String controlKey = "CONTROL";


    public BSPProcessorPellet() {

        System.out.println("****************BSP Processor*****************");

    }


    public void invoke(FIterator fIterator, FMapEmitter fMapEmitter) {
        String currentKey = "";
        Map<Long, List<SubGraphMessage>> messageGroups = new HashMap<Long, List<SubGraphMessage>>();

        while (true) {

            try {
                Object o = fIterator.next();

                if (o instanceof BSPMessage) {


                    BSPMessage message = (BSPMessage) o;

                    if (message.getType() == BSPMessage.INIT) {
                        String data = new String(message.getData());
                        //Format
                        // application_jar,app_class,number_of_processors,graphId,uri
                        String[] parts = data.split(",");

                        String applicationJar = parts[0];
                        String appClass = parts[1];
                        String graphId = parts[3];
                        URI nameNodeUri = URI.create(parts[4]);
                        implClass = loadClass(appClass, applicationJar);
                        init(graphId, nameNodeUri);
                        logger.info("Goper initized successfully with " + appClass +
                                " for graph : " + graphId);
                        continue;
                    } else if (message.getType() == BSPMessage.HALT) {

                        subGraphMap.clear();
                        subGraphProcessorMap.clear();
                        currentSuperStep = 0;
                        currentIteration = 0;
                        partition = null;
                        partList.clear();
                        partList = new ArrayList<Integer>();
                        pool.shutdownNow();
                        continue;
                    }


                    if ((currentSuperStep == 0) && (currentIteration == 0)) {
                        StatLogger.getInstance().log("START," + System.currentTimeMillis());
                    }
                    // Handle a control message that arrived from manager
                    if (message.getType() == BSPMessage.CTRL) {

                        currentSuperStep = message.getSuperStep();

                        boolean stepUp = false;
                        if (message.isIterative()) {

                            if (currentIteration != message.getIteration()) {
                                stepUp = true;
                            }
                            currentIteration = message.getIteration();
                        }

                        // since we're using a linkedhashmap, the order in which we insert into the map
                        // is the order in which the keys are returned (used when scheduling subgraphs)
                        Iterator<Long> subgraphIter = subGraphMap.keySet().iterator();

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
                                subGraphProcessorMap.put(subgraph.getId(), processor);
                            }

                            SubGraphTaskRunner task = new SubGraphTaskRunner(partition, subgraph, processor,
                                    fMapEmitter, semaphore, messageGroups.get(subgraph.getId()), stepUp);
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


                        boolean halt = true;
                        for (GopherSubGraph processor : subGraphProcessorMap.values()) {
                            if (!processor.isVoteToHalt()) {
                                halt = false;
                                break;
                            }
                            if (processor.isMessagesSent()) {
                                bspMessage.setData(("" + Boolean.toString(true)).getBytes());
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
                        if (currentKey == null) {
                            currentKey = "" + partition.getId();
                        }

                        SubGraphMessage msg = (SubGraphMessage) readObject(message.getData());

                        if (msg.hasTargetSubgraph()) {
                            // we know which sub-graph in partition to route message to
                            //String partitionId = currentKey.split(":")[0];

                            ISubgraph subGraph = partition.getSubgraph(msg.getTargetSubgraph());
                            if (!subGraphMap.containsKey(msg.getTargetSubgraph())) {
                                subGraphMap.put(subGraph.getId(), subGraph);
                            }

                            if (messageGroups.containsKey(subGraph.getId())) {
                                messageGroups.get(subGraph.getId()).add(msg);
                            } else {
                                ArrayList<SubGraphMessage> mList = new ArrayList<SubGraphMessage>();
                                mList.add(msg);
                                messageGroups.put(subGraph.getId(), mList);
                            }
                        } else {
                            for (ISubgraph subGraph : partition) {

                                if (!subGraphMap.containsKey(subGraph.getId())) {
                                    subGraphMap.put(subGraph.getId(), subGraph);
                                }

                                if (messageGroups.containsKey(subGraph.getId()) &&
                                        messageGroups.get(subGraph.getId()) != null) {
                                    messageGroups.get(subGraph.getId()).add(msg);
                                } else {
                                    ArrayList<SubGraphMessage> mList = new ArrayList<SubGraphMessage>();
                                    mList.add(msg);
                                    messageGroups.put(subGraph.getId(), mList);
                                }

                            }

                        }
                    }
                } else { // o not BSPMessage
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


    private static Class loadClass(String className, String jarName) {
        try {


            logger.info("Loading Application jars from directory : " + JAR_DIR);
            File jarDir = new File(JAR_DIR);
            Class clazz = null;
            if (jarDir.isDirectory()) {

                File[] jars = jarDir.listFiles();

                for (File jar : jars) {

                    if (jar.getName().equals(jarName)) {
                        URL jarFileUrl = new URL("file:" + jar.getAbsolutePath());
                        ClassLoader classLoader = new URLClassLoader(new URL[]{jarFileUrl});
                        clazz = classLoader.loadClass(className);
                        return clazz;
                    }
                }
            } else {
                throw new RuntimeException("Error while Reading from Application Jar location :" +
                        JAR_DIR);
            }

            if (clazz == null) {
                clazz = Class.forName(className);
            }
            return clazz;
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Exception at class loading", e);
            throw new RuntimeException(e);
        }
    }


    private void init(String graphId, URI uri) {
        INameNode nameNode = new RemoteNameNode(uri);
        try {
            for (URI u : nameNode.getDataNodes()) {
                if (URIHelper.isLocalURI(u)) {
                    dataNode = DataNode.create(u);
                    IntCollection partitions = dataNode.getLocalPartitions(graphId);
                    int partId = partitions.iterator().nextInt();
                    partition = dataNode.loadLocalPartition(graphId, partId);

                    for (int pid : nameNode.getPartitionDirectory().getPartitions(graphId)) {

                        this.partList.add(pid);
                    }
                    break;
                }
            }


            if (partition == null) {
                throw new RuntimeException("UnExpected Error Graph " + graphId + "@" + uri +
                        " does not exist");
            }

            for (ISubgraph subgraph : partition) {
                subGraphMap.put(subgraph.getId(), subgraph);

                for(ITemplateVertex rv : subgraph.remoteVertices()) {
                    subgraphToPartitionMap.put(rv.getRemoteSubgraphId(),rv.getRemotePartitionId());
                }
            }

            int concurrentSubGraphSlots = (Runtime.getRuntime().availableProcessors() - 1) * 2;
            concurrentSubGraphSlots = concurrentSubGraphSlots <= 0 ? 1 : concurrentSubGraphSlots;
            pool = new ForkJoinPool(concurrentSubGraphSlots);


        } catch (IOException e) {
            throw new RuntimeException(e);
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

        private boolean stepUp = false;

        private FMapEmitter emitter;
        private Semaphore semaphore;

        private SubGraphTaskRunner(IPartition partition, ISubgraph subgraph, GopherSubGraph processor,
                                   FMapEmitter emitter, Semaphore semaphore,
                                   List<SubGraphMessage> messages, boolean stepUp) {
            this.partition = partition;
            this.subgraph = subgraph;
            this.processor = processor;
            this.emitter = emitter;
            this.semaphore = semaphore;

            if (messages == null) {
                this.messages = new ArrayList<SubGraphMessage>();
            } else {
                this.messages = messages;
            }
            this.stepUp = stepUp;
        }

        @Override
        public void run() {

            // initialize processor
            Map<Long, List<SubGraphMessage>> outBuffer = new HashMap<Long, List<SubGraphMessage>>();
            if (processor.isCleanedUp()) {
                processor.init(partition, subgraph, partList);
            }


            processor.setSuperStep(currentSuperStep);
            processor.setOutBuffer(outBuffer);
            processor.setIteration(currentIteration);
            processor.setMessagesSent(false);


            // call compute, as long as it did not halt previously and it has no new messages
            if (!(processor.isVoteToHalt() && messages.isEmpty()) || stepUp) {
                processor.setVoteToHalt(false);
                long st = System.currentTimeMillis();
                processor.compute(messages);

                StatLogger.getInstance().log("COMPUTE," + (System.currentTimeMillis() - st) + "," +
                        currentSuperStep + "," + currentIteration);
            }

            // Emit output messages to remote workers
            //TODO pipeLineThis by adding emiting
            if (!outBuffer.isEmpty()) {
                Iterator<Long> longIt = outBuffer.keySet().iterator();
                while (longIt.hasNext()) {
                    long partId = longIt.next();

                    if (partId != GopherSubGraph.SUBGRAPH_LIST_KEY) {
                        for (SubGraphMessage m : outBuffer.get(partId)) {
                            byte[] data = BitConverter.getBytes(m);
                            BSPMessage bspMessage = new BSPMessage();
                            bspMessage.setSuperStep(currentSuperStep);
                            bspMessage.setType(BSPMessage.DATA);
                            bspMessage.setKey("" + partId);
                            bspMessage.setData(data);
                            emitter.emit("" + partId, bspMessage);


                        }
                    } else {

                        for (SubGraphMessage m : outBuffer.get(GopherSubGraph.SUBGRAPH_LIST_KEY)) {

                            byte[] data = BitConverter.getBytes(m);
                            BSPMessage bspMessage = new BSPMessage();
                            bspMessage.setSuperStep(currentSuperStep);
                            bspMessage.setType(BSPMessage.DATA);
                            bspMessage.setKey("" + partId);
                            bspMessage.setData(data);

                            long remoteSubgraphId = m.getTargetSubgraph();
                            long targetPartitionId = subgraphToPartitionMap.get(remoteSubgraphId);

                            emitter.emit("" + targetPartitionId, bspMessage);


                        }

                    }
                }
            }

            outBuffer.clear();

            // signal to BSPProcessor that this task is completed
            semaphore.release();
        }
    }

}
