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

import edu.usc.pgroup.floe.impl.manager.infraManager.infraHandler.GopherInfraHandler;
import org.apache.axiom.om.OMAbstractFactory;
import org.apache.axiom.om.OMElement;
import org.apache.axiom.om.OMFactory;

import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import java.io.IOException;
import java.io.OutputStream;

public class FloeGraphGenerator {

    private OMFactory factory;

    /**
     * Generate a Floe Graph for BSP given number of processors
     * @param numberOfProcessors
     * @return OMElement of the graph
     */
    public OMElement createFloe(int numberOfProcessors) {

        factory = OMAbstractFactory.getOMFactory();
        OMElement floeRoot = factory.createOMElement(FloeGraphXMLConstants.FLOE_GRAPH);

        OMElement nodes = createNodeSet(numberOfProcessors);
        floeRoot.addChild(nodes);
        floeRoot.addChild(createEdgeSet(numberOfProcessors));

        return floeRoot;
    }


    private OMElement createNodeSet(int numberOfNodes) {

        OMElement nodesRoot = factory.createOMElement(FloeGraphXMLConstants.NODES);

        for (int i = 0; i < numberOfNodes; i++) {
            OMElement node = createProcessorNode(numberOfNodes, i + 1);
            nodesRoot.addChild(node);
        }

        nodesRoot.addChild(createNode(numberOfNodes + 1, FloeGraphXMLConstants.CONTROLLER_CLASS));
        nodesRoot.addChild(createNode(numberOfNodes + 2, FloeGraphXMLConstants.PASS_THROUGH_CLASS));

        return nodesRoot;
    }

    private OMElement createEdgeSet(int numberOfNodes) {
        OMElement edgeSetRoot = factory.createOMElement(FloeGraphXMLConstants.EDGES);

        //FWD to Control wiring
        OMElement edge = createEdge(String.valueOf(numberOfNodes + 2), "out", String.valueOf(numberOfNodes + 1), "in");
        edgeSetRoot.addChild(edge);

        //Control to Processor wiring

        for (int i = 0; i < numberOfNodes; i++) {
            edge = createEdge(String.valueOf(numberOfNodes + 1), "out", String.valueOf(i + 1), "in");
            edgeSetRoot.addChild(edge);
        }


        //Processor to control wiring

        for (int i = 0; i < numberOfNodes; i++) {
            edge = createEdge(String.valueOf(i + 1), "CONTROL", String.valueOf(numberOfNodes + 1), "in");
            edgeSetRoot.addChild(edge);
        }

        //Self wiring for processors

        for (int i = 0; i < numberOfNodes; i++) {
            edge = createEdge(String.valueOf(i + 1), String.valueOf(i + 1), String.valueOf(i + 1),
                    "in");
            edgeSetRoot.addChild(edge);
        }

        //Wiring between processors

        for (int i = 0; i < numberOfNodes; i++) {

            for (int j = 0; j < numberOfNodes; j++) {

                if (i == j) {
                    continue;
                }

                edge = createEdge(String.valueOf(i + 1), String.valueOf(j + 1), String.valueOf(j + 1),
                        "in");
                edgeSetRoot.addChild(edge);

            }
        }


        return edgeSetRoot;
    }


    private OMElement createEdge(String sourceNode, String sourcePort, String sinkNode,
                                 String sinkPort) {

        OMElement edge = factory.createOMElement(FloeGraphXMLConstants.EDGE);

        OMElement source = factory.createOMElement(FloeGraphXMLConstants.SOURCE);
        edge.addChild(source);
        OMElement nodeId = factory.createOMElement(FloeGraphXMLConstants.NODE_ID);
        nodeId.setText(sourceNode);
        source.addChild(nodeId);
        OMElement port = factory.createOMElement(FloeGraphXMLConstants.EDGE_PORT);
        port.setText(sourcePort);
        source.addChild(port);

        OMElement sink = factory.createOMElement(FloeGraphXMLConstants.SINK);
        edge.addChild(sink);
        nodeId = factory.createOMElement(FloeGraphXMLConstants.NODE_ID);
        nodeId.setText(sinkNode);
        sink.addChild(nodeId);
        port = factory.createOMElement(FloeGraphXMLConstants.EDGE_PORT);
        port.setText(sinkPort);
        sink.addChild(port);


        OMElement channelBehaviourType = factory.createOMElement(FloeGraphXMLConstants.CHANNEL_BEHAVIOUR_TYPE);
        channelBehaviourType.setText(FloeGraphXMLConstants.PUSH);
        edge.addChild(channelBehaviourType);

        OMElement channelTransportType = factory.createOMElement(FloeGraphXMLConstants.CHANNEL_TRANSPORT_TYPE);
        channelTransportType.setText(FloeGraphXMLConstants.SOCKET);
        edge.addChild(channelTransportType);

        return edge;
    }

    private OMElement createNode(int nodeId, String pelletCls) {

        OMElement nodesRoot = factory.createOMElement(FloeGraphXMLConstants.NODE);

        //<nodeId>
        OMElement id = factory.createOMElement(FloeGraphXMLConstants.NODE_ID);
        id.setText(String.valueOf(nodeId));
        nodesRoot.addChild(id);

        OMElement pelletType = factory.createOMElement(FloeGraphXMLConstants.PELLET_TYPE);
        pelletType.setText(pelletCls);
        nodesRoot.addChild(pelletType);

        //        <Resource>
//             <numberOfCores>1</numberOfCores>
//             <Configuration>
//                  <param key="key">val</param>
//               </Configuration>
//        </Resource>

        OMElement resourceRoot = factory.createOMElement(FloeGraphXMLConstants.RESOURCE);
        OMElement numberOfCores = factory.createOMElement(FloeGraphXMLConstants.NUM_OF_CORES);
        OMElement configuration = factory.createOMElement(FloeGraphXMLConstants.CONFIGURATION);

        OMElement gopherParam = factory.createOMElement(FloeGraphXMLConstants.PARAM);
        gopherParam.setText(FloeGraphXMLConstants.resourceTypeGopher);
        gopherParam.addAttribute(FloeGraphXMLConstants.KEY,FloeGraphXMLConstants.resourceType,null);
        configuration.addChild(gopherParam);


        OMElement param = factory.createOMElement(FloeGraphXMLConstants.PARAM);
        param.setText("N/A");
        param.addAttribute(FloeGraphXMLConstants.KEY, GopherInfraHandler.PARTITION,null);
        configuration.addChild(param);
        numberOfCores.setText("1");
        resourceRoot.addChild(numberOfCores);
        resourceRoot.addChild(configuration);
        nodesRoot.addChild(resourceRoot);





        //<InPorts>
        //<Port>
        //<portName>in</portName >
        //</Port>
        //</InPorts>

        OMElement inports = factory.createOMElement(FloeGraphXMLConstants.IN_PORTS);
        OMElement port = factory.createOMElement(FloeGraphXMLConstants.PORT);
        inports.addChild(port);
        OMElement portName = factory.createOMElement(FloeGraphXMLConstants.PORT_NAME);
        portName.setText("in");
        port.addChild(portName);
        nodesRoot.addChild(inports);

        OMElement outPorts = factory.createOMElement(FloeGraphXMLConstants.OUT_PORTS);
        nodesRoot.addChild(outPorts);
        port = factory.createOMElement(FloeGraphXMLConstants.PORT);
        outPorts.addChild(port);
        portName = factory.createOMElement(FloeGraphXMLConstants.PORT_NAME);
        portName.setText("out");
        port.addChild(portName);

        return nodesRoot;

    }


    private OMElement createProcessorNode(int numberOfNodes, int nodeId) {



        //<Node>
        OMElement nodesRoot = factory.createOMElement(FloeGraphXMLConstants.NODE);

        //<nodeId>
        OMElement id = factory.createOMElement(FloeGraphXMLConstants.NODE_ID);
        id.setText(String.valueOf(nodeId));
        nodesRoot.addChild(id);

        OMElement pelletType = factory.createOMElement(FloeGraphXMLConstants.PELLET_TYPE);
        pelletType.setText(FloeGraphXMLConstants.PROCESSOR_CLASS);
        nodesRoot.addChild(pelletType);

        OMElement resourceRoot = factory.createOMElement(FloeGraphXMLConstants.RESOURCE);
        OMElement numberOfCores = factory.createOMElement(FloeGraphXMLConstants.NUM_OF_CORES);
        OMElement configuration = factory.createOMElement(FloeGraphXMLConstants.CONFIGURATION);
        OMElement gopherParam = factory.createOMElement(FloeGraphXMLConstants.PARAM);
        gopherParam.setText(FloeGraphXMLConstants.resourceTypeGopher);
        gopherParam.addAttribute(FloeGraphXMLConstants.KEY,FloeGraphXMLConstants.resourceType,null);
        configuration.addChild(gopherParam);

        OMElement param = factory.createOMElement(FloeGraphXMLConstants.PARAM);
        param.setText(String.valueOf(nodeId));
        param.addAttribute(FloeGraphXMLConstants.KEY, GopherInfraHandler.PARTITION,null);
        configuration.addChild(param);

        numberOfCores.setText("1");
        resourceRoot.addChild(numberOfCores);
        resourceRoot.addChild(configuration);
        nodesRoot.addChild(resourceRoot);

        //<InPorts>
        //<Port>
        //<portName>in</portName >
        //</Port>
        //</InPorts>

        OMElement inports = factory.createOMElement(FloeGraphXMLConstants.IN_PORTS);
        OMElement port = factory.createOMElement(FloeGraphXMLConstants.PORT);
        inports.addChild(port);
        OMElement portName = factory.createOMElement(FloeGraphXMLConstants.PORT_NAME);
        portName.setText("in");
        port.addChild(portName);
        nodesRoot.addChild(inports);

        OMElement outPorts = factory.createOMElement(FloeGraphXMLConstants.OUT_PORTS);
        nodesRoot.addChild(outPorts);
        for (int i = 0; i < numberOfNodes; i++) {
            port = factory.createOMElement(FloeGraphXMLConstants.PORT);
            outPorts.addChild(port);

            portName = factory.createOMElement(FloeGraphXMLConstants.PORT_NAME);
            portName.setText(String.valueOf(i + 1));
            port.addChild(portName);


        }
        port = factory.createOMElement(FloeGraphXMLConstants.PORT);
        outPorts.addChild(port);

        portName = factory.createOMElement(FloeGraphXMLConstants.PORT_NAME);
        portName.setText("CONTROL");
        port.addChild(portName);


        return nodesRoot;
    }

    /**
     *  Serialize given OMElement to a output stream
     * @param element  OMElement to serialize
     * @param stream   Output stream to write
     * @throws IOException
     */
    public void serialize(OMElement element, OutputStream stream) throws IOException {
        try {
            XMLStreamWriter writer =
                    XMLOutputFactory.newInstance().createXMLStreamWriter(stream);
            element.serialize(writer);
        } catch (XMLStreamException e) {
            throw new IOException(e);
        }
    }


    public static void main(String[] args) throws XMLStreamException {
        new FloeGraphGenerator().createFloe(1).serialize(System.out);
    }

}