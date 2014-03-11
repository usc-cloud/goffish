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

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import edu.usc.goffish.gofs.INameNode;
import edu.usc.goffish.gofs.namenode.DataNode;
import edu.usc.goffish.gofs.namenode.RemoteNameNode;
import edu.usc.goffish.gofs.tools.GoFSFormat;
import edu.usc.goffish.gofs.tools.GoFSNameNodeClient;
import edu.usc.pgroup.floe.api.communication.TransportInfoBase;
import edu.usc.pgroup.floe.api.framework.StartFloeInfo;
import edu.usc.pgroup.floe.api.framework.floegraph.FloeGraph;
import org.apache.axiom.om.OMElement;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.configuration.XMLConfiguration;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Unmarshaller;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.util.Iterator;
import java.util.List;

public class DeploymentTool {


    private static final int COORDINATOR_PORT = 45000;
    private static final int MANAGER_PORT = 45001;


    public static final String FLOE_MANAGER = "gopher.floe.manager";
    public static final String FLOE_COORDINATOR = "gopher.floe.coordinator";

    public static final String DATA_FLOW_HOST = "gopher.floe.dataflow.host";
    public static final String DATA_FLOW_DATA_PORT = "gopher.floe.dataflow.port";
    public static final String DATA_FLOW_CONTROL_PORT = "gopher.floe.dataflow.control";


    public static final String CONFIG_FILE_PATH = "gopher-config.xml";

    public static final String FLOE_GRAPH_PATH = "floe-bsp.xml";


    public void setup(String gofsConfig, String managerHost, String coordinatorHost, boolean persist) throws
            IOException, ConfigurationException {

        int numberOfProcessors = 0;

        PropertiesConfiguration gofs = new PropertiesConfiguration(gofsConfig);
        gofs.load();
        String nameNodeRestAPI = gofs.getString(GoFSFormat.GOFS_NAMENODE_LOCATION_KEY);

        INameNode nameNode = new RemoteNameNode(URI.create(nameNodeRestAPI));
        numberOfProcessors = nameNode.getDataNodes().size();

        // generate flow graph.
        FloeGraphGenerator generator = new FloeGraphGenerator();
        OMElement xmlOm = generator.createFloe(numberOfProcessors);
        xmlOm.build();
        try {
            JAXBContext ctx = JAXBContext.newInstance(FloeGraph.class);
            Unmarshaller um = ctx.createUnmarshaller();
            FloeGraph floe = (FloeGraph) um.unmarshal(xmlOm.getXMLStreamReader());

            if (persist) {
                FileOutputStream fileOutputStream = new FileOutputStream(new File(FLOE_GRAPH_PATH));
                ctx.createMarshaller().marshal(floe, fileOutputStream);
            }
            DefaultClientConfig config = new DefaultClientConfig();
            config.getProperties().put(ClientConfig.FEATURE_DISABLE_XML_SECURITY,
                    true);
            config.getFeatures().put(ClientConfig.FEATURE_DISABLE_XML_SECURITY, true);


            Client c = Client.create(config);
            WebResource r = c.resource("http://" + coordinatorHost + ":" + COORDINATOR_PORT +
                    "/Coordinator/createFloe");
            c.getProperties().put(ClientConfig.PROPERTY_FOLLOW_REDIRECTS, true);
            ClientResponse response;
            c.getProperties().put(ClientConfig.PROPERTY_FOLLOW_REDIRECTS, true);
            response = r.post(ClientResponse.class, floe);
            StartFloeInfo startFloeInfo = response.getEntity(StartFloeInfo.class);

            createClientConfig(startFloeInfo, managerHost, coordinatorHost);


        } catch (Exception e) {
            throw new RuntimeException(e);
        }


    }


    private void createClientConfig(StartFloeInfo info, String managerHost, String coordinatorHost)
            throws IOException, ConfigurationException {

        XMLConfiguration config = new XMLConfiguration();
        config.setRootElementName("GopherConfiguration");

        config.setProperty(FLOE_MANAGER, managerHost + ":" + MANAGER_PORT);
        config.setProperty(FLOE_COORDINATOR, coordinatorHost + ":" + COORDINATOR_PORT);

        info.getSourceInfo().sourceNodeTransport.values();

        for (List<TransportInfoBase> b : info.getSourceInfo().sourceNodeTransport.values()) {
            for (TransportInfoBase base : b) {
                String host = base.getParams().get("hostAddress");
                int dataPort = Integer.parseInt(base.getParams().get("tcpListenerPort"));

                int controlPort = Integer.parseInt(base.getControlChannelInfo().getParams().
                        get("tcpListenerPort"));

                config.setProperty(DATA_FLOW_HOST, host);
                config.setProperty(DATA_FLOW_DATA_PORT, dataPort);
                config.setProperty(DATA_FLOW_CONTROL_PORT, controlPort);
                break;
            }
            break;
        }

        config.save(new FileWriter(CONFIG_FILE_PATH));

    }


    public void listDataNodes(String gofsConfig) throws ConfigurationException, IOException {

        PropertiesConfiguration configuration = new PropertiesConfiguration(gofsConfig);
        configuration.load();

        String nameNodeUri = configuration.getString(GoFSFormat.GOFS_NAMENODE_LOCATION_KEY);

        INameNode nameNode = new RemoteNameNode(URI.create(nameNodeUri));

        System.out.println("**********************DATA NODE LIST**********************");
        for (Iterator<URI> it = nameNode.getDataNodes().iterator(); it.hasNext(); ) {
            URI uri = it.next();
            System.out.println(uri);
        }
    }

    public static void main(String[] args) throws IOException, ConfigurationException {

        if ("list".equals(args[0])) {
            if (args.length == 2) {

                new DeploymentTool().listDataNodes(args[1]);

            } else {
                System.out.println("Invalid arguments - Expected args : " +
                        "arg[0]=list arg[1]=gofsConfig file path");
            }


        } else if ("setup".equals(args[0])) {

            if (args.length >= 4) {

                if (args.length == 4) {
                    new DeploymentTool().setup(args[1], args[2], args[3],true);
                } else if (args.length == 5) {
                    new DeploymentTool().setup(args[1], args[2], args[3],
                            Boolean.parseBoolean(args[4]));
                } else {
                    System.out.println("Invalid arguments - Expected args : " +
                            "arg[0]=setup arg[1]=gofsConfig file path arg[2]=managerHost " +
                            "arg[3]=coordinatorHost <Optional> arg[4]=true if needs to persist the floe graph");
                }
            } else {
                System.out.println("Invalid arguments - Expected args : " +
                        "arg[0]=setup arg[1]=gofsConfig file path arg[2]=managerHost " +
                        "arg[3]=coordinatorHost <Optional> arg[4]=true if needs to persist the floe graph");
            }
        }
    }


}
