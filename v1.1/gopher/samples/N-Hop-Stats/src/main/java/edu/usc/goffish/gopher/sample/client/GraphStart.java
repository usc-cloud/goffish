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
package edu.usc.goffish.gopher.sample.client;

import com.sun.jersey.api.client.*;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import edu.usc.pgroup.floe.api.communication.TransportInfoBase;
import edu.usc.pgroup.floe.api.framework.StartFloeInfo;
import edu.usc.pgroup.floe.api.framework.floegraph.FloeGraph;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Unmarshaller;
import javax.xml.transform.stream.StreamSource;
import java.io.File;
import java.util.List;

public class GraphStart {


    public static void main(String[] args)  {
        if(args.length != 2) {
            throw new RuntimeException("args[0]=coordinator Host " +" arg[1]=floe-graph.xml path");
        }

        String coordinatorHost =args[0];
        String filePath = args[1];
        try
        {
            //Get The Node List From File
            JAXBContext ctx = JAXBContext.newInstance(FloeGraph.class);
            Unmarshaller um = ctx.createUnmarshaller();
            FloeGraph fg = (FloeGraph) um.unmarshal(new StreamSource(
                    new File(filePath)));

            DefaultClientConfig config = new DefaultClientConfig();
            config.getProperties().put(ClientConfig.FEATURE_DISABLE_XML_SECURITY,
                    true);
            config.getFeatures().put(ClientConfig.FEATURE_DISABLE_XML_SECURITY, true);


            com.sun.jersey.api.client.Client c = com.sun.jersey.api.client.Client.create(config);
            WebResource r = c.resource("http://"+coordinatorHost + ":45000/Coordinator/createFloe");
            c.getProperties().put(ClientConfig.PROPERTY_FOLLOW_REDIRECTS, true);
            ClientResponse response ;
            c.getProperties().put(ClientConfig.PROPERTY_FOLLOW_REDIRECTS, true);
            response = r.post(ClientResponse.class, fg);
            StartFloeInfo startFloeInfo = response.getEntity(StartFloeInfo.class);

            System.out.println("Floe id : " + startFloeInfo.getFloeID());
            for(List<TransportInfoBase> b : startFloeInfo.getSourceInfo().sourceNodeTransport.values()) {
                for(TransportInfoBase base : b) {
                    System.out.println("Channel Info");
                    base.printConnectionInfoDetails();
                    System.out.println("Control Channel info");
                    base.getControlChannelInfo().printConnectionInfoDetails();
                }
            }
        }
        catch(Exception ex)
        {
            ex.printStackTrace();
        }

    }
}
