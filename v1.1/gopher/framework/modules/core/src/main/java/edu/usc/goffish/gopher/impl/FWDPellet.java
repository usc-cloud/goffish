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
import edu.usc.pgroup.floe.api.framework.pelletmodels.Pellet;
import edu.usc.pgroup.floe.api.state.StateObject;

import java.util.logging.Logger;

/**
 * <class>FWDPellet</class> is the entry point for messages where users can trigger the workflow.
 */
public class FWDPellet implements Pellet {

    public Object invoke(Object o, StateObject stateObject) {

        BSPMessage msg = (BSPMessage)o;
        Logger.getLogger(this.getClass().getName()).info("Message forwared + " + msg.getKey() +
                "  Type : " + msg.getType());
        return o;
    }
}
