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

import edu.usc.goffish.gopher.bsp.BSPMessage;
import edu.usc.goffish.gopher.impl.util.StatLogger;
import edu.usc.pgroup.floe.api.exception.LandmarkException;
import edu.usc.pgroup.floe.api.exception.LandmarkPauseException;
import edu.usc.pgroup.floe.api.framework.pelletmodels.StreamInStreamOutPellet;
import edu.usc.pgroup.floe.api.state.StateObject;
import edu.usc.pgroup.floe.api.stream.FEmitter;
import edu.usc.pgroup.floe.api.stream.FIterator;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * <class>ControlPellet</class> coordinates the BSP super steps
 */
public class ControlPellet implements StreamInStreamOutPellet {

    private static Logger logger = Logger.getLogger(ControlPellet.class.getName());

    private int numberOfProcessors = -1;

    private static int currentSuperStep = 0;

    private static int currentIteration = 0;

    private boolean isIterative = false;

    private int numberOfIterations = 1;

    private long lastSuperStepTime;

    private long lastIterationTime;


    private enum State {
        halt,init,running
    }

    private State currentState = State.halt;


    public ControlPellet() {

    }

    public void invoke(FIterator fIterator, FEmitter fEmitter, StateObject stateObject) {
        //   boolean isHalt = false;
        int haltVotes = 0;
        int syncMessageCount = 0;

        while (true) {
            try {
                Object o = fIterator.next();

                // next() could timeout and return null
                if (o == null) {
                    continue;
                }

                // we expect a message of type BSPMessage 
                if (o instanceof BSPMessage) {
                    BSPMessage msg = (BSPMessage) o;

                    if (msg.getType() == BSPMessage.INIT) {

                        if(currentState != State.halt) {
                            logger.severe("Invalid Message received : " + new String(msg.getData())
                                    + " Control Pellet is in an Invalid state " + currentState);

                            continue;
                        }


                        String data = new String(msg.getData());
                        //Format
                        // application_jar,app_class,number_of_processors,graphId,url
                        String[] parts = data.split(",");
                        numberOfProcessors = Integer.parseInt(parts[2]);
                        currentState = State.init;
                        fEmitter.emit(msg);
                        continue;
                    } else if (msg.getType() == BSPMessage.DATA) {
                        /**
                         * Any data messages received when in init state are forwarded to the
                         * Processor pellets.
                         */

                        if(currentState != State.init) {
                            logger.severe("Invalid Message received : " + new String(msg.getData())
                                    + " Control Pellet is in an Invalid state " + currentState);
                            continue;
                        }

                        fEmitter.emit(msg);
                    } else if (msg.getType() == BSPMessage.CTRL &&
                            BSPMessage.INIT_ITERATION.equals(msg.getTag())) {

                        if(currentState == State.init) {
                            currentState = State.running;
                        }

                        if(currentState != State.running) {
                            logger.severe("Invalid Message received : " + new String(msg.getData())
                                    + " Control Pellet is in an Invalid state " + currentState);
                            continue;
                        }

                        if(msg.isIterative()) {
                            numberOfIterations = msg.getNumberOfIterations();
                        }

                        fEmitter.emit(msg);
                        continue;
                    }



                    System.out.println("BSPCTRL current SS :" + currentSuperStep + " MSG SS" +
                            msg.getSuperStep() + " Iteration " + msg.getIteration() + " VOTE : " + msg.isVoteToHalt());
                    int superStep = msg.getSuperStep();
                    if (superStep == currentSuperStep) {
                        syncMessageCount++;
                        if (lastSuperStepTime == 0) {
                            lastIterationTime = lastSuperStepTime = System.currentTimeMillis();
                        }


                        // reached end of BSP application. Halt.
                        byte[] data = msg.getData();
                        boolean isActive = false;
                        if (data != null) {
                            String dStr = new String(data);
                            if (Boolean.toString(true).equals(dStr.trim())) {
                                isActive = true;
                            }
                        }
                        if (msg.isVoteToHalt() && !isActive) {
                            System.out.println("*********************Control got vote to halt #" +
                                    haltVotes + " ******************************");
                            if (++haltVotes == numberOfProcessors) {

                                if (!isIterative) {
                                    System.out.println("**********************Control got ALL vote to halts. Halting...************************");
                                    StatLogger.getInstance().log("END," +
                                            System.currentTimeMillis());
                                    BSPMessage message = new BSPMessage();
                                    message.setType(BSPMessage.HALT);
                                    fEmitter.emit(message);
                                    continue;
                                } else {

                                    long duration = System.currentTimeMillis() - lastIterationTime;

                                    StatLogger.getInstance().log("ITERATION," + currentIteration + "," + duration);

                                    if (currentIteration < (numberOfIterations - 1)) {
                                        System.out.println("**********************Iteration " + currentIteration + " Done *****************");
                                        currentIteration++;
                                        currentSuperStep = -1;
                                    } else {

                                        System.out.println("All " + numberOfIterations + " Iterations are done! Halting");
                                        StatLogger.getInstance().log("END," +
                                                System.currentTimeMillis());
                                        BSPMessage message = new BSPMessage();
                                        message.setType(BSPMessage.HALT);
                                        fEmitter.emit(message);
                                        continue;
                                    }

                                }
                            }
                        }

                        // reached barrier. release barrier messages for next superstep.
                        if (syncMessageCount == numberOfProcessors) {
                            long duration = System.currentTimeMillis() - lastSuperStepTime;

                            StatLogger.getInstance().log("SUPER," + currentSuperStep + "," + duration);

                            // send async message to all
                            BSPMessage barrierMessage = new BSPMessage();
                            barrierMessage.setType(BSPMessage.CTRL);
                            barrierMessage.setSuperStep(++currentSuperStep);
                            barrierMessage.setIterative(isIterative);
                            barrierMessage.setIteration(currentIteration);
                            fEmitter.emit(barrierMessage);

                            // reset counters
                            haltVotes = 0;
                            syncMessageCount = 0;
                        }

                    } else {
                        logger.severe("Unexpected BSP Control Message. Invalid superstep seen: " + superStep + " when expecting superstep: " + currentSuperStep);
                        throw new RuntimeException("Unexpected Control Message. Invalid superstep seen: " + superStep + " when expecting superstep: " + currentSuperStep);
                    }

                } else { // not BSP message 
                    logger.severe("Unexpected Message not of BSP Message type: " + o);
                }
            } catch (LandmarkException e) {
                e.printStackTrace();
            } catch (LandmarkPauseException e) {
                e.printStackTrace();
            }
        }
    }
}
