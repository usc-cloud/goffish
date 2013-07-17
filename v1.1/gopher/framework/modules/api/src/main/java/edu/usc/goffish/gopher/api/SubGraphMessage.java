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

import java.io.Serializable;


/**
 * <class>SubGraphMessage</class> is the Message that user can use to send messages between partitions/sub-graphs
 * @param <T>  data type
 */
public class SubGraphMessage<T> implements Serializable {

    private long targetVertex;

    private String tag;

    private byte[] data;

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
    public SubGraphMessage<T> addTag(String tag) {
        this.tag = tag;
        return this;
    }

    /**
     * Set the target vertex id this messages is referring to.
     * This vertex id will be used at the remote partition to determine the sub-graph that
     * this message will be dispatched to.
     * This message will be dispatched to the sub-graph which contain the target vertex
     * @param id target remote vertex id
     * @return Current Message instance.
     */
    public SubGraphMessage<T> addTargetVertex(long id) {
        this.targetVertex = id;
        return this;
    }

    /**
     * Get the Target Remote vertex id
     * @return  target remote vertex id
     */
    public long getTargetVertex() {
        return targetVertex;
    }

    /**
     * Get the application specific message tag.
     * @return message tag.
     */
    public String getTag() {
        return tag;
    }

    /**
     * Get the message data as a byte[]
     * @return data as a byte[]
     */
    public byte[] getData() {
        return data;
    }
}
