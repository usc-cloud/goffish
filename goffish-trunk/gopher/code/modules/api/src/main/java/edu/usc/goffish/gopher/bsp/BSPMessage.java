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
package edu.usc.goffish.gopher.bsp;

import java.io.Serializable;

public class BSPMessage implements Serializable {

    public static final int DATA = 1;

    public static final int CTRL = 2;

    public static final int INIT = 3;

    public static final int HALT = 4;

    public static final String INIT_ITERATION = "INIT_INTERATION";

    private int type;

    private byte[]data;

    private int superStep;

    private int iteration;

    private int numberOfIterations;

    private boolean reduce;

    private String tag;

    private String key;

    private boolean voteToHalt = false;

    private boolean appHalt = false;

    private boolean iterative = false;

    public boolean isVoteToHalt() {
        return voteToHalt;
    }

    public void setVoteToHalt(boolean voteToHalt) {
        this.voteToHalt = voteToHalt;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public byte[] getData() {
        return data;
    }

    public void setData(byte[] data) {
        this.data = data;
    }

    public int getSuperStep() {
        return superStep;
    }

    public void setSuperStep(int superStep) {
        this.superStep = superStep;
    }

    public String getTag() {
        return tag;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }

    public int getIteration() {
        return iteration;
    }

    public void setIteration(int iteration) {
        this.iteration = iteration;
    }

    public boolean isIterative() {
        return iterative;
    }

    public void setIterative(boolean iterative) {
        this.iterative = iterative;
    }

    public int getNumberOfIterations() {
        return numberOfIterations;
    }

    public void setNumberOfIterations(int numberOfIterations) {
        this.numberOfIterations = numberOfIterations;
    }


    public boolean isReduceStep() {
        return reduce;
    }

    public void setReduce(boolean reduce) {
        this.reduce = reduce;
    }

    public boolean isAppHalt() {
        return appHalt;
    }

    public void setAppHalt(boolean appHalt) {
        this.appHalt = appHalt;
    }
}
