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
package edu.usc.goffish.gopher.impl.client;

import javax.xml.namespace.QName;

final class FloeGraphXMLConstants {

    /**
     * Implementation class names used in floe graph
     */
    public static final String PROCESSOR_CLASS="edu.usc.goffish.gopher.impl.BSPProcessorPellet";

    public static final String CONTROLLER_CLASS="edu.usc.goffish.gopher.impl.ControlPellet";

    public static final String PASS_THROUGH_CLASS="edu.usc.goffish.gopher.impl.FWDPellet";


    /**
     * Channel config
     */
    public static final String PUSH = "Push";

    public static final String SOCKET = "Socket";


    /**
     * XML elements
     */

    public static final QName FLOE_GRAPH = QName.valueOf("FloeGraph");

    public static final QName NODES = QName.valueOf("Nodes");

    public static final QName NODE = QName.valueOf("Node");

    public static final QName NODE_ID= QName.valueOf("nodeId");

    public static final QName PELLET_TYPE = QName.valueOf("pelletType");

    public static final QName RESOURCE = QName.valueOf("Resource");

    public static final QName NUM_OF_CORES = QName.valueOf("numberOfCores");

    public static final QName CONFIGURATION = QName.valueOf("Configuration");

    public static final QName PARAM = QName.valueOf("param");

    public static final QName IN_PORTS = QName.valueOf("InPorts");

    public static final QName PORT = QName.valueOf("Port");

    public static final QName PORT_NAME = QName.valueOf("portName");

    public static final QName OUT_PORTS = QName.valueOf("OutputPorts");

    public static final QName EDGES = QName.valueOf("Edges");

    public static final QName EDGE = QName.valueOf("Edge");

    public static final QName SOURCE = QName.valueOf("source");

    public static final QName EDGE_PORT = QName.valueOf("port");

    public static final QName SINK = QName.valueOf("sink");

    public static final QName CHANNEL_BEHAVIOUR_TYPE = QName.valueOf("channelBehaviourType");

    public static final QName CHANNEL_TRANSPORT_TYPE = QName.valueOf("channelTransportType");

    public static final String KEY = "key";




}
