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
package edu.usc.goffish.gopher.api;

import edu.usc.goffish.gofs.IPartition;
import edu.usc.goffish.gofs.ISubgraph;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * <class>GopherSubGraph</class>  Provide the Subgraph centric api for Gopher.
 * User will extend this class and implement compute() to implement the user logic.
 */
public abstract class GopherSubGraph {

    public static final long SUBGRAPH_LIST_KEY = -1l;

    private boolean voteToHalt;

    private boolean haultApp;

    private List<SubGraphMessage> reduceList = new ArrayList<>();

    private Map<Long, List<SubGraphMessage>> outBuffer;

    private boolean isCleanedUp = true;

    private int iteration;

    protected int superStep;

    protected IPartition partition;

    protected ISubgraph subgraph;

    protected List<Integer> partitions;

    private boolean messagesSent = false;



    /**
     * Initialize the sub-graph by providing partition and sub-graph instances to be used by the subgraph.
     *
     * @param partition current partition
     * @param subgraph  current subgraph
     */
    public final void init(IPartition partition, ISubgraph subgraph,
                           List<Integer> partitions) {
        if (partition == null || subgraph == null) {
            throw new IllegalArgumentException("Invalid parameters. parameters can't be null");
        }

        this.partition = partition;
        this.partitions = partitions;
        this.subgraph = subgraph;

    }

    /**
     * User implementation logic goes here.
     * To send message to a another partition use {@link #sendMessage(long, SubGraphMessage)}} see
     * {@link SubGraphMessage} for more details about the message format.
     * To signal that current logic is done processing use {@link #voteToHalt()}
     *
     * @param messageList List of SubGraphMessage which is intended for this sub graph.
     */
    public abstract void compute(List<SubGraphMessage> messageList);

    /**
     * User implementation logic goes here.
     *
     * @param messageList  list of message indented for
     */
    public void reduce(List<SubGraphMessage> messageList) {
        voteToHalt();
    }


    /**
     * Get the Current super step.
     *
     * @return
     */
    public final int getSuperStep() {
        return superStep;
    }

    /**
     * Signal that this subgraph finished processing. The system will come to a halt state once all the subgraphs come
     * to an halt state.
     */
    public final void voteToHalt() {
        this.voteToHalt = true;
    }

    /**
     * Send Message to a given partition.
     *
     * @param partitionId target partition
     * @param message     data message for that partition
     */
    public final void sendMessage(long partitionId, SubGraphMessage message) {
        if (this.outBuffer == null) {
            throw new RuntimeException("Unexpcted Error  , SubGraph not initialized properly");
        }
        synchronized (outBuffer) {
            if (outBuffer.containsKey(partitionId)) {
                outBuffer.get(partitionId).add(message);
            } else {
                ArrayList<SubGraphMessage> mList = new ArrayList<SubGraphMessage>();
                mList.add(message);
                outBuffer.put(partitionId, mList);
            }
        }
        messagesSent = true;
    }


    public final void sendMessageToReduceStep(SubGraphMessage message) {
        synchronized (reduceList) {

            reduceList.add(message);
        }
    }


    /**
     * Send Message to a subgraph
     *
     * @param message subgraph message
     */
    public final void sendMessage(SubGraphMessage message) {
        if (this.outBuffer == null) {
            throw new RuntimeException("Unexpcted Error  , SubGraph not initialized properly");
        }

        if (!message.hasTargetSubgraph()) {
            throw new RuntimeException("Routing details not avaiable , subgraph id not set");
        }

        synchronized (outBuffer) {
            if (outBuffer.containsKey(SUBGRAPH_LIST_KEY)) {
                outBuffer.get(SUBGRAPH_LIST_KEY).add(message);
            } else {
                ArrayList<SubGraphMessage> mList = new ArrayList<SubGraphMessage>();
                mList.add(message);
                outBuffer.put(SUBGRAPH_LIST_KEY, mList);
            }
        }

        messagesSent = true;
    }

    /**
     * Check whether the current sub-graph is in halt state
     *
     * @return
     */
    public final boolean isVoteToHalt() {
        return voteToHalt;
    }

    /**
     * Halt the app,
     * Signal that this is the last iteration
     *
     */
    public void haultApp(){
        haultApp = true;
    }

    public boolean isHaultApp() {
        return haultApp;
    }

    public void setHaultApp(boolean haultApp) {
        this.haultApp = haultApp;
    }

    /**
     * Clean up the subgraph from memory
     */
    public void cleanup() {
        this.subgraph = null;
        this.partition = null;
        this.isCleanedUp = true;
        System.gc();
    }


    public boolean isCleanedUp() {
        return this.isCleanedUp;
    }

    public void setSuperStep(int superStep) {
        this.superStep = superStep;
    }

    /**
     * Get Current Iteration (Meta-Step)
     *
     * @return
     */
    public int getIteration() {
        return iteration;
    }

    public void setIteration(int iteration) {
        this.iteration = iteration;
    }

    public void setVoteToHalt(boolean voteToHalt) {
        this.voteToHalt = voteToHalt;
    }

    public void setOutBuffer(Map<Long, List<SubGraphMessage>> outBuffer) {
        this.outBuffer = outBuffer;
    }

    public boolean isMessagesSent() {
        return messagesSent;
    }

    public void setMessagesSent(boolean messagesSent) {
        this.messagesSent = messagesSent;
    }

    public List<SubGraphMessage> getReduceList(){
        return this.reduceList;
    }

}
