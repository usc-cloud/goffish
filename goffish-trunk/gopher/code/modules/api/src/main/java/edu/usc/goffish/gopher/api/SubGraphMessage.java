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

import java.io.Serializable;


/**
 * <class>SubGraphMessage</class> is the Message that user can use to send messages between partitions/sub-graphs
 */
public class SubGraphMessage implements Serializable {

    private long targetVertex = Long.MIN_VALUE;

    private long targetSubgraph = Long.MIN_VALUE;

    private String tag;

    private boolean hasTargetSubgraph = false;

    private byte[] data;

    private static final long serialVersionUID = 2787697398409320177l;

    /**
     * Create SubGraphMessage providing the data as a byte[]
     * @param data data serialized to byte[] format;
     */
    public SubGraphMessage(byte[] data) {
        this.data = data;
    }

    /**
     * Add any application specific tag
     * @param tag application specific tag
     * @return  Current Message Instance
     */
    public SubGraphMessage addTag(String tag) {
        this.tag = tag;
        return this;
    }

    /**
     * Set the subgraph id of the target subgraph for this message
     * @param targetSubgraph subgraph id of target subgraph
     */
    public void setTargetSubgraph(long targetSubgraph) {
        this.targetSubgraph = targetSubgraph;
        this.hasTargetSubgraph = true;
    }

    public long getTargetSubgraph() {
        return targetSubgraph;
    }

    /**
     * Get the application specific message tag.
     * @return message tag.
     */
    public String getTag() {
        return tag;
    }

    public boolean hasTargetSubgraph() {
        return hasTargetSubgraph;
    }


    /**
     * Get the message data as a byte[]
     * @return data as a byte[]
     */
    public byte[] getData() {
        return data;
    }
}
