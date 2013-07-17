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
package edu.usc.goffish.tools.json;

import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.container.httpserver.HttpServerFactory;
import com.sun.jersey.api.core.PackagesResourceConfig;
import com.sun.jersey.api.core.ResourceConfig;
import com.sun.jersey.json.impl.provider.entity.JSONArrayProvider;
import com.sun.net.httpserver.HttpServer;
import edu.usc.goffish.gofs.INameNode;
import edu.usc.goffish.gofs.IPartition;
import edu.usc.goffish.gofs.namenode.FileNameNode;
import edu.usc.goffish.gofs.slice.FileStorageManager;
import edu.usc.goffish.gofs.slice.JavaSliceSerializer;
import edu.usc.goffish.gofs.slice.SliceManager;

import javax.ws.rs.core.UriBuilder;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.Socket;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

public class SimpleServer {

    private static Logger logger = Logger.getLogger("GOFS REST SERVER");

    private ResourceConfig resourceConfig;

        private HttpServer httpServer;

    private int port = 9998;


    private static String configFilePath = "server.properties";
    private static final String restPkg = "edu.usc.goffish.tools.json.service";
    private static final String PORT = "server.port";
    private static final String GRAPH_ID = "gofs.graphId";
    private static final String PARTITION_ID = "gofs.partitionId";
    private static final String FILE_NAME_NODE_PATH = "gofs.fileNameNodePath";
    private static final String SLICE_PATH = "gofs.slicePath";

    private String graphId;
    private String partitionId;
    private String nameNodePath;
    private String slicePath;


    private SliceManager sliceManager;
    private IPartition partition;

    private Properties properties = new Properties();

    public static void main(String[] args) {
        if (args.length != 1) {
            String msg = "server configuration file not specifies using " + configFilePath +
                    " as default path";
            handleException(msg, null);
        } else {
            configFilePath = args[0];
        }

        SimpleServer server = new SimpleServer();
        server.init(configFilePath);
        server.start();

    }

    public void init(String configFilePath) {
        try {
            /**
             * Loading Config
             */
            properties.load(new FileInputStream(configFilePath));

            int port = Integer.parseInt(properties.getProperty(PORT));
            String graphId = properties.getProperty(GRAPH_ID);
            int partitionId = Integer.parseInt(properties.getProperty(PARTITION_ID));
            String fileNodePath = properties.getProperty(FILE_NAME_NODE_PATH);
            String slicePath = properties.getProperty(SLICE_PATH);


            /**
             * Lording  Partition
             */

            INameNode nameNode = new FileNameNode(Paths.get(fileNodePath));
            URI currentPartURI = nameNode.getPartitionMapping(graphId,partitionId);

            Path tempPath = Paths.get(slicePath);
            sliceManager = new SliceManager(UUID.fromString(currentPartURI.getFragment()), new JavaSliceSerializer(),
                    new FileStorageManager(tempPath));

            partition = sliceManager.readPartition();

            ResourceHolder.getInstance().setPartition(partition);
            ResourceHolder.getInstance().setSliceManager(sliceManager);


            /**
             * Starting Service
             */
            this.resourceConfig = new PackagesResourceConfig(restPkg);

            resourceConfig.getFeatures().put(ClientConfig.FEATURE_DISABLE_XML_SECURITY, true);
            resourceConfig.getProperties().put(ClientConfig.FEATURE_DISABLE_XML_SECURITY, "true");
            resourceConfig.getClasses().add(JSONArrayProvider.class);
            resourceConfig.getClasses().add(JSONArrayProvider.class);
            this.httpServer = HttpServerFactory.create(getURL(port), resourceConfig);

        } catch (IOException e) {
            String msg = "Error while starting Rest service for coordinator";
            handleException(msg, e);
        }
    }


    public void stop() {
        httpServer.stop(0);
        logger.info("Rest service stopped...");
    }

    public void start() {

        this.httpServer.start();
        logger.info("Rest Service started at " + httpServer.getAddress());
    }


    private static void handleException(String msg, Exception e) {
        logger.log(Level.SEVERE, msg, e);
    }


    private String getHostAddress() throws IOException {

        Socket socket = new Socket("google.com", 80);
        String host = socket.getInetAddress().getHostAddress();
        socket.close();
        return host;

    }


    private URI getURL(int port) {
        try {
            return UriBuilder.fromUri("http://" + getHostAddress() + "/").port(port).build();
        } catch (IOException e) {
            handleException("Unexpected Error while generating service url", e);
            return UriBuilder.fromUri("http://localhost/").port(port).build();
        }
    }


}
