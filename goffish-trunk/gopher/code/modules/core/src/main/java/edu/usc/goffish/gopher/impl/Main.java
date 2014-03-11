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

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import edu.usc.goffish.gofs.IDataNode;
import edu.usc.goffish.gofs.INameNode;
import edu.usc.goffish.gofs.ISubgraph;
import edu.usc.goffish.gofs.namenode.DataNode;
import edu.usc.goffish.gofs.namenode.RemoteNameNode;
import edu.usc.goffish.gofs.util.URIHelper;
import edu.usc.pgroup.floe.api.framework.Container;
import edu.usc.pgroup.floe.impl.FloeRuntimeEnvironment;
import edu.usc.pgroup.floe.impl.manager.infraManager.infraHandler.GopherInfraHandler;
import edu.usc.pgroup.floe.util.Constants;
import it.unimi.dsi.fastutil.ints.IntCollection;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.util.Properties;
import java.util.concurrent.ForkJoinPool;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Main {

    private static final Logger log = Logger.getLogger(Main.class.getName());

    private static final String CONFIG_FILE = "conf" + File.separator + "Container.properties";

    public static void main(String[] args) {

        Properties properties = new Properties();
        try {
            properties.load(new FileInputStream(CONFIG_FILE));
        } catch (IOException e) {
            String message = "Error while loading Container Configuration from " + CONFIG_FILE +
                    " Cause -" + e.getCause();
            log.warning(message);
        }


        if (args.length == 4) {

            PropertiesConfiguration propertiesConfiguration;
            String url = null;

            URI uri = URI.create(args[3]);

            String dataDir = uri.getPath();

            String currentHost = uri.getHost();
            try {

                propertiesConfiguration = new PropertiesConfiguration(dataDir + "/gofs.config");
                propertiesConfiguration.load();
                url = (String) propertiesConfiguration.getString(DataNode.DATANODE_NAMENODE_LOCATION_KEY);

            } catch (ConfigurationException e) {

                String message = " Error while reading gofs-config cause -" + e.getCause();
                handleException(message);
            }

                URI nameNodeUri = URI.create(url);

                INameNode nameNode = new RemoteNameNode(nameNodeUri);
                int partition = -1;
                try {
                    for (URI u : nameNode.getDataNodes()) {
                        if (URIHelper.isLocalURI(u)) {
                            IDataNode dataNode = DataNode.create(u);
                            IntCollection partitions = dataNode.getLocalPartitions(args[2]);
                            partition = partitions.iterator().nextInt();
                            break;
                        }
                    }

             if(partition == -1) {
                String message = "Partition not loaded from uri : " + nameNodeUri;
                handleException(message);
             }

             properties.setProperty(GopherInfraHandler.PARTITION, String.valueOf(partition));

            } catch (Exception e) {
                String message = "Error while loading Partitions from " + nameNodeUri +
                        " Cause -" + e.getMessage();
                e.printStackTrace();
                handleException(message);
            }

            properties.setProperty(Constants.STATIC_PELLET_COUNT, String.valueOf(1));
            FloeRuntimeEnvironment environment = FloeRuntimeEnvironment.getEnvironment();
            environment.setSystemConfig(properties);
            properties.setProperty(Constants.CURRET_HOST,currentHost);
            String managerHost = args[0];
            int managerPort = Integer.parseInt(args[1]);

            Container container = environment.getContainer();
            container.setManager(managerHost,managerPort);

            DefaultClientConfig config = new DefaultClientConfig();
            config.getProperties().put(ClientConfig.FEATURE_DISABLE_XML_SECURITY,
                    true);
            config.getFeatures().put(ClientConfig.FEATURE_DISABLE_XML_SECURITY, true);

            Client c = Client.create(config);

            if (managerHost == null || managerPort == 0) {
                handleException("Manager Host / Port have to be configured in " + args[0]);
            }


            WebResource r = c.resource("http://" + managerHost + ":" + managerPort + "/Manager/addContainerInfo/Container=" +
                    container.getContainerInfo().getContainerId() + "/Host=" + container.getContainerInfo().getContainerHost());
            c.getProperties().put(ClientConfig.PROPERTY_FOLLOW_REDIRECTS, true);
            r.post();
            log.log(Level.INFO, "Container started  " );




        } else {

            String message = "Invalid arguments , arg[0]=Manager host, " +
                    "arg[1] = mamanger port,arg[2]=graph id,arg[3]=partition uri";


            message += "\n Current Arguments...." + args.length + "\n";
            for(int i = 0;i<args.length;i++) {
                message+= "arg "  + i + " : " + args[i] + "\n";
            }


            handleException(message);

        }


    }

    private static void handleException(String message) {
        log.log(Level.SEVERE, message);
        throw new RuntimeException(message);
    }
}
