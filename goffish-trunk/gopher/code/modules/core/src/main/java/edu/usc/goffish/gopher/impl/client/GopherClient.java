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

import edu.usc.goffish.gofs.INameNode;
import edu.usc.goffish.gofs.namenode.RemoteNameNode;
import edu.usc.goffish.gofs.tools.GoFSFormat;
import edu.usc.goffish.gopher.api.SubGraphMessage;
import edu.usc.goffish.gopher.bsp.BSPMessage;
import edu.usc.pgroup.floe.api.communication.*;
import edu.usc.pgroup.floe.api.framework.floegraph.Node;
import edu.usc.pgroup.floe.api.util.BitConverter;
import edu.usc.pgroup.floe.impl.communication.MessageImpl;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.configuration.XMLConfiguration;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Gopher client
 */
public class GopherClient {


    private PropertiesConfiguration gofs;

    private XMLConfiguration gopher;


    private INameNode nameNode;

    private static final String seperator = ",";


    private static Logger logger = Logger.getLogger(GopherClient.class.getName());

    private Sender sender = null;

    /**
     * Initialize the system to run a new application.
     * @param gopherConfig Gopher Configuration file Path
     * @param gofsConfig  Gofs Configuration File Path
     * @param graphId   Id of the graph
     * @param jarFile  application jar file name
     * @param clzz    application class name
     */
    public void initialize(String gopherConfig, String gofsConfig, String graphId, String jarFile, String clzz) {
        /**
         * Assume application jar is copied already
         * Send the init command with data // application_jar:app_class:number_of_processors:graphId,uri
         */

        try {
            gofs = new PropertiesConfiguration(gofsConfig);
            gopher = new XMLConfiguration(gopherConfig);

            gofs.load();
            gopher.load();

            String nameNodeUri = gofs.getString(GoFSFormat.GOFS_NAMENODE_LOCATION_KEY);

            nameNode = new RemoteNameNode(URI.create(nameNodeUri));
            int numberOfProcessors = nameNode.getDataNodes().size();

            String data = jarFile + seperator + clzz + seperator + numberOfProcessors +
                    seperator + graphId + seperator + nameNodeUri;
            System.out.println("Sending Init message : " + data);
            BSPMessage message = new BSPMessage();
            message.setData(data.getBytes());
            message.setType(BSPMessage.INIT);


            String location = gopher.getString(DeploymentTool.DATA_FLOW_HOST);
            int port = gopher.getInt(DeploymentTool.DATA_FLOW_DATA_PORT);
            int controlPort = gopher.getInt(DeploymentTool.DATA_FLOW_CONTROL_PORT);


            TransportInfoBase otherEndTransportInfo = new TransportInfoBase();
            otherEndTransportInfo.getParams().put(TransportConstants.HOST_ADDR, location);
            otherEndTransportInfo.getParams().put(TransportConstants.TCP_LISTEN_PORT, String.valueOf(port));


            TransportInfoBase otherEndControlInfo = new TransportInfoBase();
            otherEndControlInfo.getParams().put(TransportConstants.HOST_ADDR, location);
            otherEndControlInfo.getParams().put(TransportConstants.TCP_LISTEN_PORT, String.valueOf(controlPort));

            otherEndTransportInfo.setControlChannelInfo(otherEndControlInfo);

            Node.Port otherEndport = new Node.Port();
            otherEndport.setPortName("in");
            otherEndport.setTransportType("TCP");
            otherEndport.setDataTransferMode("Push");

            otherEndport.setTransportInfo(otherEndTransportInfo);

            Map<String, Object> params = new HashMap<String, Object>();
            params.put("KEY", null);
            params.put("QUEUE", null);
            params.put("SERVER_SIDE", "false");

            sender = SenderFactory.createDefaultSender();
            sender.init(params);
            sender.connect(otherEndport, null);

            Message tempMessage = new MessageImpl();
            tempMessage.putPayload(BitConverter.getBytes(message));

            sender.send(tempMessage);


        } catch (ConfigurationException e) {
            String message = "Error Processing configuration";
            handleException(message, e);
        } catch (IOException e) {
            String message = "I/O Error while initializing Gopher client";
            handleException(message, e);
        } catch (ClassNotFoundException e) {
            String message = "Unexpected error while Creating Sender ";
            handleException(message, e);
        } catch (InstantiationException e) {
            String message = "Unexpected error while Creating Sender ";
            handleException(message, e);
        } catch (IllegalAccessException e) {
            String message = "Unexpected error while Creating Sender ";
            handleException(message, e);
        }


    }

    /**
     * Sent initial data required by the application
     *
     * @param message SubGraph message which contains the data
     * @throws NullPointerException if message is null for data is empty
     */
    public void sendData(SubGraphMessage message) throws NullPointerException {

        if (message == null) {
            throw new NullPointerException("Invalid arguments : message is null ");
        }

        if (message.getData() == null) {
            throw new NullPointerException("Message with empty data");
        }

        if (sender == null) {
            String msg = "Gopher client is not initialized properly";
            handleException(msg, new ExceptionInInitializerError(msg));
        }


        BSPMessage frameworkMsg = new BSPMessage();
        frameworkMsg.setType(BSPMessage.DATA);
        frameworkMsg.setData(BitConverter.getBytes(message));

        Message tempMessage = new MessageImpl();
        tempMessage.putPayload(BitConverter.getBytes(frameworkMsg));

        sender.send(tempMessage);

    }

    /**
     * Send initial data required by the application
     *
     * @param messages list of subgraph messages which contains the initial data
     * @throws NullPointerException if a message is null or contains no data.
     *                              Valid message will be sent
     */
    public void sendData(Iterable<SubGraphMessage> messages) throws NullPointerException {

        boolean errorOccurred = false;
        for (SubGraphMessage message : messages) {
            try {
                sendData(message);
            } catch (NullPointerException e) {
                errorOccurred = true;
            }
        }

        if (errorOccurred) {
            throw new NullPointerException("Messag collection contains one or more null messages or " +
                    "empty data, those message were dropped. ");
        }
    }

    /**
     * Send the start signal to gopher.Which will start executing
     */
    public void start() {

        if (sender == null) {
            String msg = "Gopher client is not initialized properly";
            handleException(msg, new ExceptionInInitializerError(msg));
        }

        BSPMessage message = new BSPMessage();
        message.setType(BSPMessage.CTRL);
        message.setSuperStep(0);
        message.setTag(BSPMessage.INIT_ITERATION);

        Message tempMessage = new MessageImpl();
        tempMessage.putPayload(BitConverter.getBytes(message));

        sender.send(tempMessage);

    }

    public void start(int numberOfIterations) {

        if (sender == null) {
            String msg = "Gopher client is not initialized properly";
            handleException(msg, new ExceptionInInitializerError(msg));
        }

        if(numberOfIterations ==0) {
            start();
            return;
        }

        BSPMessage message = new BSPMessage();
        message.setType(BSPMessage.CTRL);
        message.setSuperStep(0);
        message.setTag(BSPMessage.INIT_ITERATION);
        message.setIterative(true);
        message.setNumberOfIterations(numberOfIterations);

        Message tempMessage = new MessageImpl();
        tempMessage.putPayload(BitConverter.getBytes(message));

        sender.send(tempMessage);



    }


    private static void handleException(String message, Throwable e) {
        logger.log(Level.SEVERE, message + "- cause " + e.getCause());
        throw new RuntimeException(e);

    }

    /**
     * Close the client to cleanup the resources
     */
    public void close(){
        gofs = null;
        gopher=null;
        sender.stop();
        sender = null;
    }


    /**
     * Simple Commandline version for Gopher client
     * @param args  gopherConfig gofsConfig graphId jarFile clzz data iterations
     */
    public static void main(String[] args) {

        GopherClient client = new GopherClient();

        if(args.length == 6 || args.length == 7) {
            client.initialize(args[0],args[1],args[2],args[3],args[4]);

            if(!"NILL".equals(args[5])) {
                SubGraphMessage message = new SubGraphMessage(args[5].getBytes());
                client.sendData(message);
            }


            if(args.length == 6) {
                client.start();
            } else {
                client.start(Integer.parseInt(args[6]));
            }

        } else {
            System.out.println("Invalid Arguments, Useage : " +
                    "java -cp ... edu.usc.goffish.gopher.impl.client.GopherClient " +
                    "gopherConfig gofsConfig graphId jarFile clzz data (optional)iterations ");
            throw new IllegalArgumentException("\"Invalid Arguments , Usage \" +\n" +
                    "                    \"java -cp ... edu.usc.goffish.gopher.impl.client.GopherClient \" +\n" +
                    "                    \"gopherConfig gofsConfig graphId jarFile clzz data (optional)iterations \"");
        }


    }


}

