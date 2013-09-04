GoFS Documentation
------------------

GoFS is structured as a Maven project with an API module ('gofs-api') and an implementation module ('gofs-core') and all the usual maven instructions apply. After building and packaging GoFS you should be left with a gofs zip file. This zip file can be extracted at the target location and contains the following elements:

bin/
	-GoFS jar files
	-GoFS dependencies
	-GoFS helper scripts
conf/
	-An example config file for formatting a new GoFS installation

Once the zip file has been extracted into the above directories, you may run scripts from the bin/ folder. In order to use GoFS generally three steps are needed.

1. A Name Node must be chosen.
2. The GoFSFormat script is used to create a new GoFS installation.
3. Graph data is deployed to an installation.

Name Node
---------

A name node is an interface that tracks all the metadata for a particular GoFS installation. At the moment GoFS ships with a REST server based namenode, which can be run through the GoFSNameNode script. This script accepts a URI argument and attempts to start a name node REST server at this URI. Optionally a file may also be specified which is used to save name node state. It is recommended you always specify a save file or name node state may be lost if the process is killed. Once a name node exists it is referenced through a java class type and a URI. The default type is edu.usc.goffish.gofs.namenode.RemoteNameNode which can communicate with a remote REST server based name node.

To start a name node server on the local host on port 9998:
>GoFSNameNode http://localhost:9998


Formatting
----------

Once you have an appropriate name node set up and have a URI to reference it, you may format a new GoFS installation. To do so, edit the gofs.config file found in the conf directory. The first two options are the type and uri of the name node. Next you must specify a set of data nodes in the installation. Data nodes are locations where GoFS installations will be created and controlled from the name node. For example, to specify an installation on a remote computer 'datanode-1' at the path '/home/user1/gofs/' use file://datanode-1/home/user1.gofs . Paths specified on a machine must always be absolute and end with a slash to reference a folder. Finally, specify a serializer type that is used to serialize graph information at each data node. The default Kryo serializer is generally sufficient.

Once the config file has been setup, the GoFSFormat script may be run, which uses passwordless SSH and SCP to setup each data node. This script has options to specify a config file in a non default location, and to copy GoFS binaries to each data node (useful for running scripts on the data nodes).

After this script completes, each data node will be formatted, and the name node will be updated with the information to use the installation.


In order to format a new installation, the user must first edit the conf file in the conf directory to specify the particulars of the installation.

but requires the user to edit the config file found in the conf/ folder first. Within this config file, you must specify the name node this installation will be using, list all machines used in the GoFS cluster under the datanode section, and specify the method of serialization at each datanode.

Deployment
----------

Once the file system has been setup as above, the next step is to deploy a graph to the data nodes to prepare for applications to be run on GoFS. This document assumes a GML template file and any applicable instance files already exist.

As a first step, the user must prepare an XML file listing the path to the template GML and the paths to all instance GML files. Once this file is prepared, the GoFSDeployGraph script is invoked with arguments for the name node type, the name node uri, the graph id (a string used to differentiate between multiple graphs deployed to the same installation) the number of partitions to partition the graph into to split over datanodes, and finally the above mentioned GML file. For example:

> GoFSDeploy edu.usc.goffish.gofs.namenode.RemoteNameNode http://localhost:9998 "graph1" 4 "/home/user1/gofs/gmllocations.xml"

From here the script will partition the graph, perform some preprocessing, serialize the graph, and distribute it to the data nodes set up in the formatting step above. Multiple graphs may be deployed to the same GoFS installation, an installation comprises a name node responsible for storing metadata and a set of data nodes responsible for storing data. There are also a variety of optional arguments to controlling the particulars of deployment.