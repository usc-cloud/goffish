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
package edu.usc.pgroup.goffish.gopher.sample.client;


import edu.usc.goffish.gopher.bsp.BSPMessage;
import edu.usc.pgroup.floe.api.communication.*;
import edu.usc.pgroup.floe.api.framework.floegraph.Node;
import edu.usc.pgroup.floe.api.util.BitConverter;


import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class    Client {

    public static void main(String[] args) throws ClassNotFoundException, InstantiationException, IllegalAccessException {

        if(args.length != 3) {
            throw new RuntimeException("args[0]=Host to connect + arg[1]=data port arg[2]=control port");
        }

        String location = args[0];

        int port = Integer.parseInt(args[1]);
        int controlPort = Integer.parseInt(args[2]);

        TransportInfoBase otherEndTransportInfo = new TransportInfoBase();
        otherEndTransportInfo.getParams().put(TransportConstants.HOST_ADDR,location);
        otherEndTransportInfo.getParams().put(TransportConstants.TCP_LISTEN_PORT, String.valueOf(port));


        TransportInfoBase otherEndControlInfo = new TransportInfoBase();
        otherEndControlInfo.getParams().put(TransportConstants.HOST_ADDR,location);
        otherEndControlInfo.getParams().put(TransportConstants.TCP_LISTEN_PORT, String.valueOf(controlPort));

        otherEndTransportInfo.setControlChannelInfo(otherEndControlInfo);

        Node.Port otherEndport = new Node.Port();
        otherEndport.setPortName("in");
        otherEndport.setTransportType("TCP");
        otherEndport.setDataTransferMode("Push");

        otherEndport.setTransportInfo(otherEndTransportInfo);

        BlockingQueue<byte[]> tempQueue = new LinkedBlockingQueue<byte[]>();

        Map<String, Object> params = new HashMap<String, Object>();
        params.put("KEY", null);
        params.put("QUEUE", null);
        params.put("SERVER_SIDE", "false");

        Sender sender = SenderFactory.createDefaultSender();
        sender.init(params);
        sender.connect(otherEndport, null);

        Message<byte[]> tempMessage = MessageFactory.createDefaultMessage();


        BSPMessage msg = new BSPMessage();
        msg.setSuperStep(0);
        msg.setType(BSPMessage.CTRL);

        byte[] payLoad = BitConverter.getBytes(msg);

        tempMessage.putPayload(payLoad);


        sender.send(tempMessage);
    }
}
