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

import edu.usc.goffish.gopher.bsp.BSPMessage;
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
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

/**
 * <class>ControlPellet</class> coordinates the BSP super steps
 */
public class ControlPellet implements StreamInStreamOutPellet {

    private static Logger logger = Logger.getLogger(ControlPellet.class.getName());

    private  int numberOfProcessors = 2;
    private static int currentSuperStep = 0;

    private static String configFilePath ="bsp-config"+ File.separator +"manager.properties";


    public static final String NUMBER_OF_PROCESSORS = "numberOfProcessors";

    public ControlPellet() {

        Properties properties = new Properties();
        try {


            properties.load(new FileInputStream(configFilePath));
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("Error while initialising the Control Pellet , Reason : " +
                    e.getLocalizedMessage(),e);
        }

        String numOfCores = properties.getProperty(NUMBER_OF_PROCESSORS);
        numberOfProcessors = Integer.parseInt(numOfCores);

    }

    public void invoke(FIterator fIterator, FEmitter fEmitter, StateObject stateObject) {
        boolean isHalt = false;
        int haltVotes = 0;
        int syncMessageCount = 0;

        while (!isHalt) {
            try {
                Object o = fIterator.next();

                // next() could timeout and return null
                if(o == null) {
                    continue;
                }
                
                // we expect a message of type BSPMessage 
                if(o instanceof BSPMessage) {
                    BSPMessage msg = (BSPMessage)o;
                    int superStep = msg.getSuperStep();
                    if(superStep == currentSuperStep) {
                        syncMessageCount++;

                        // reached end of BSP application. Halt.
                        if(msg.isVoteToHalt()) {
                            System.out.println("*********************Control got vote to halt #" +
                                    haltVotes + " ******************************");
                            if(++haltVotes == numberOfProcessors) {
                                isHalt = true;
                                System.out.println("**********************Control got ALL vote to halts. Halting...************************");
                                continue;
                            }
                        }

                        // reached barrier. release barrier messages for next superstep.
                        if(syncMessageCount == numberOfProcessors){
                            // send async message to all
                        	BSPMessage barrierMessage = new BSPMessage();
                            barrierMessage.setType(BSPMessage.CTRL);
                            barrierMessage.setSuperStep(++currentSuperStep);
                            fEmitter.emit(barrierMessage);
                            
                            // reset counters
                            haltVotes = 0;
                            syncMessageCount = 0;
                        }
                        
                    } else { 
                        logger.severe("Unexpected BSP Control Message. Invalid superstep seen: " + superStep + " when expecting superstep: "+ currentSuperStep);
                        throw new RuntimeException("Unexpected Control Message. Invalid superstep seen: " + superStep + " when expecting superstep: "+ currentSuperStep);
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
