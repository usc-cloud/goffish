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
package edu.usc.goffish.gopher.api;

import edu.usc.goffish.gofs.IPartition;
import edu.usc.goffish.gofs.ISubgraph;
import edu.usc.goffish.gofs.slice.SliceManager;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * <class>GopherSubGraph</class>  Provide the Subgraph centric api for Gopher.
 * User will extend this class and implement compute() to implement the user logic.
 */
public abstract class GopherSubGraph {

    private boolean voteToHalt;

    private Map<Long,List<SubGraphMessage>> outBuffer;

    private boolean isCleanedUp=true;

    protected int superStep;

    protected IPartition partition;

    protected ISubgraph subgraph;

    protected SliceManager sliceManager;

    protected List<Integer> partitions;

    /**
     * Initialize the sub-graph by providing partition and sub-graph instances to be used by the subgraph.
     * @param partition current partition
     * @param subgraph  current subgraph
     * @param outBuffer message buffer which holds the messages
     */
    public final void init(IPartition partition,ISubgraph subgraph,SliceManager sliceManager ,List<Integer> partitions ,
                           Map<Long,List<SubGraphMessage>> outBuffer) {
        if(partition == null || subgraph == null || outBuffer == null) {
            throw new IllegalArgumentException("Invalid parameters. parameters can't be null");
        }

        this.partition = partition;
        this.partitions = partitions;
        this.subgraph = subgraph;
        this.outBuffer = outBuffer;
        this.sliceManager = sliceManager;
    }

    /**
     * User implementation logic goes here.
     * To send message to a another partition use {@link #sentMessage(long, SubGraphMessage)}} see
     * {@link SubGraphMessage} for more details about the message format.
     * To signal that current logic is done processing use {@link #voteToHalt()}
     * @param messageList List of SubGraphMessage which is intended for this sub graph.
     */
    public abstract void compute(List<SubGraphMessage> messageList);

    /**
     * Get the Current super step.
     * @return
     */
    public  final int getSuperStep() {
        return superStep;
    }

    /**
     * Signal that this subgraph finished processing. The system will come to a halt state once all the subgraphs come
     * to an halt state.
     */
    public final  void voteToHalt() {
        this.voteToHalt = true;
    }

    /**
     * Send Message to a given partition.
     * @param partitionId target partition
     * @param message data message for that partition
     */
    public final  void sentMessage(long partitionId,SubGraphMessage message) {
        if(this.outBuffer == null) {
            throw new RuntimeException("Unexpcted Error  , SubGraph not initialized properly");
        }

        if(outBuffer.containsKey(partitionId)) {
            outBuffer.get(partitionId).add(message);
        } else {
            ArrayList<SubGraphMessage> mList = new ArrayList<SubGraphMessage>();
            mList.add(message);
            outBuffer.put(partitionId,mList);
        }

    }

    /**
     * Check whether the current sub-graph is in halt state
     * @return
     */
    public final boolean isVoteToHalt() {
        return voteToHalt;
    }

    /**
     * Clean up the subgraph from memory
     */
    public void cleanup(){
        this.subgraph = null;
        this.partition = null;
        this.isCleanedUp = true;
        System.gc();
    }


    public boolean isCleanedUp(){
        return this.isCleanedUp;
    }

    public void setSuperStep(int superStep) {
        this.superStep = superStep;
    }
}
