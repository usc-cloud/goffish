*****************
  Getting Started   
 *****************

 a) Install Maven
    i. Below link explains about the installation of Maven
       http://maven.apache.org/download.cgi

 b) Build GoFS Project
    Below command will clean and build the modules of GoFS project
  % mvn clean install 

 c) After the build is successful, GoFS deployment package will be created under ~/modules/gofs-core/target directory as gofs-core-1.0-bin.zip file

 d) Extracting the zip file contents will contain 
    i.  source code of GoFS under src/main/java directory
   ii.  gofs-core-1.0.jar
  iii.  dependent jars of GoFS under /lib directory

 *******
  Usage
 *******

 ************************
  GoFS Graph Deployment
 ************************

 GoFS Graph Deployment partitions the GML Graph Template/Instances File and serializes the partititon into slice files. GoFS supports graph partitioning using Metis/Stream Partitioner. It also supports Java/Kryo serialization format. 

 GoFS deployment can be run on single machine for testing purpose or as a distributed version for production. Name node can be accessed using File/Rest Service.

****************************************************
 Pseudo-Distributed Deployment (On Single Node) 
****************************************************

 GoFS Graph Deployment can be run on single node in a pseudo-distibuted version. It partitions the Graph Template/Instances files and writes the slices on disk in same node, by creating directories that contains the slices for each partition.

 Run the below command on single machine.

  java -cp gofs-core-1.0.jar;<dependent jars> edu.usc.goffish.gofs.tools.DeployGraph [args] <namenodefile> <graphid> <numpartitions> <gmlinputfile> <locationinputfile>
  Args: [-partitioner stream|metis[:<pathtometisbinary>]|predefined:<partitioningfile>]
        [-mapper roundrobin]
        [-distributer write]
        [-serializer java|kryo]

    Options:
      The -partitioner flag selects the partioning to use. Selecting metis allows for an optional argument with
    the path to the metis binary. If not specified, the binary is assumed to be 'gpmetis' and available on the
    PATH. Selecting predefined allows for a required argument, the path to a METIS style partitioning file.
    distributed to each uri location. If predefined is selected, the value supplied for <numpartitions> will
    be ignored. If the -partitioner flag is not specified, METIS is used.
      The -mapper flag selects the mapper to use to map partitions to final locations as found in the location
    input file. If the -mapper flag is not specified, round robin mapping is used.
      The -distributer flag selects the method of distributing partitions to their final destination. Selecting
    write attempts to write files directly to their location. The location must be accessible through the file
    system, whether local, or a network share, etc...
      The -serializer flag selects the serialization format for writing slices to disk. If the -serializer
    flag is not specified, java serialization is used.
      The <namenodefile> is a file for name node storage. This file may already exist, in which case it must
    represent a valid name node. If it does not exist, the file will be created at the location given.
      The <gmlinputfile> is a text file containing a list of GML files, one per line. The first line must be the
    path to the template GML file, every following line is the path to an instance GML file.
      The <locationinputfile> is a text file containing a list of valid URIs, one per line. Each URI specifies a
    location to distribute partitions to, and is interpreted by the distributer (i.e. the SCP distributer pulls
    out a specfied port and uses it as the SCP port).

  
 GoFS can be used as Java/Jython library in your application.

 *************
 Java Library
 *************

 a) Include the gofs-core-1.0.jar and dependent jars in classpath of your project

 b) Use ISliceManager to read graph instances from disk. FileNameNode should be used to read the slice id of partition.

 Read Graph Instances(After Graph Deployment):
 Code Snippet:

 //Read the Partition Metadata Slice Id
 FileNameNode fileNameNode = new FileNameNode(<namenodefile);
 URI uri = fileNameNode.getPartitionMapping(graphId, partitionId);
 UUID sliceID = UUID.fromString(uri.getFragment());


 //Create Slice Manager
 SliceManager sliceManager = new SliceManager(sliceID, <sliceserializer>, new FileStorageManager(<slicesdirectory>));

 //Read Graph Partition
 IPartition graphPartition = sliceManager.readPartition();

 //Read Graph Instances for all the vertex and edge properties
 ISubgraphInstance graphInstances = null;
 for(ISubgraph subgraph : graphPartition){
    graphInstances = sliceManager.readInstances(subgraph, <startrange>, <endrange>, subgraph.getVertexProperties(), subgraph.getEdgeProperties());
 }


 ***************
 Jython Library
 ***************

 a) Install Jython

  Below link explains about the installation of Jython
 http://wiki.python.org/jython/InstallationInstructions

 b) Add the gofs-core-1.0.jar and dependent jars in sys.path of jython script

 c) Import the 'edu.usc.goffish.gofs.partition.gml', 'edu.usc.goffish.gofs.slice', 'edu.usc.goffish.gofs.namenode' and other necessary java packages in the script

 d) Use ISliceManager to read graph instances from disk. FileNameNode should be used to read the slice id of partition.
 Read Graph Instances(After Graph Deployment):
 Code Snippet:

 //Read the Partition Metadata Slice Id
 fileNameNode = FileNameNode(<namenodefile);
 uri = fileNameNode.getPartitionMapping(graphId, partitionId);
 sliceID = UUID.fromString(uri.getFragment());


 //Create Slice Manager
 sliceManager = new SliceManager(sliceID, <sliceserializer>, new FileStorageManager(<slicesdirectory>));

 //Read Graph Partition
 graphPartition = sliceManager.readPartition();

 //Read Graph Instances for all the vertex and edge properties
 for(ISubgraph subgraph : graphPartition){
    graphInstances = sliceManager.readInstances(subgraph, <startrange>, <endrange>, subgraph.getVertexProperties(), subgraph.getEdgeProperties());
 }

********************************
     Distributed Deployment 
*********************************

 GoFS Distributed Deployment partitions the Graph Template/Instances file and serializes the partition into slices. It copies the set of slice files for each partition to different data nodes using scp. This process is performed sequentially for each partition. The deployment should be executed at the name node.

 Run the below command on name node:

  java -cp gofs-core-1.0.jar;<dependent jars> edu.usc.goffish.gofs.tools.DeployGraph [args] <namenodefile> <graphid> <numpartitions> <gmlinputfile> <locationinputfile>
  Args: [-partitioner stream|metis[:<pathtometisbinary>]|predefined:<partitioningfile>]
        [-mapper roundrobin]
        [-distributer scp]
        [-serializer java|kryo]

    Options:
      The -partitioner flag selects the partioning to use. Selecting metis allows for an optional argument with
    the path to the metis binary. If not specified, the binary is assumed to be 'gpmetis' and available on the
    PATH. Selecting predefined allows for a required argument, the path to a METIS style partitioning file.
    distributed to each uri location. If predefined is selected, the value supplied for <numpartitions> will
    be ignored. If the -partitioner flag is not specified, METIS is used.
      The -mapper flag selects the mapper to use to map partitions to final locations as found in the location
    input file. If the -mapper flag is not specified, round robin mapping is used.
       The -distributer flag selects the method of distributing partitions to their final destination. Selecting
    scp uses SCP to transfer the files to their location. Passwordless SCP must be enabled to the destination.
    Host, path, and SCP port will be grabbed from the location URIs. If the -distributer flag is not specified,
    direct write is used.
      The -serializer flag selects the serialization format for writing slices to disk. If the -serializer
    flag is not specified, java serialization is used.
      The <namenodefile> is a file for name node storage. This file may already exist, in which case it must
    represent a valid name node. If it does not exist, the file will be created at the location given.
      The <gmlinputfile> is a text file containing a list of GML files, one per line. The first line must be the
    path to the template GML file, every following line is the path to an instance GML file.
      The <locationinputfile> is a text file containing a list of valid URIs, one per line. Each URI specifies a
    location to distribute partitions to, and is interpreted by the distributer (i.e. the SCP distributer pulls
    out a specfied port and uses it as the SCP port).

  
 GoFS can be used as Java/Jython library in your application.

 **********************
    Name Node Server
 **********************

 It is a Rest Server that maintains the name node mapping of graph and partition for each data node. Data Nodes can use the rest service to read and write mapping from the name node.

 **********
   Usage
 **********

 java -cp gofs-core-1.0.jar;<dependent jars> edu.usc.goffish.gofs.namenode.NameNodeServer [-f <locationnamenodefile>] [-h <host>:<port>]
  The <locationnamenodefile> is file for name node storage. It must represent the valid file in name node. If the 
  deployment is completed, file will be already created at the location given as part of <namenodefile> argument.
  So, Specify that file location path.
  Default <host>:<port> is localhost:9998


 *************
 Java Library
 *************

 a) Include the gofs-core-1.0.jar and dependent jars in classpath of your project

 b) Use ISliceManager to read graph instances from slices on data node. RemoteNameNode should be used to read the slice id of partition from NameNode file.

 Read Graph Instances(After Graph Deployment):
 Code Snippet:

 //Read the Partition Metadata Slice Id from RemoteNameNode
 RemoteNameNode remoteNameNode = new RemoteNameNode(<host>, <port>);
 URI uri = remoteNameNode.getPartitionMapping(graphId, partitionId);
 UUID sliceID = UUID.fromString(uri.getFragment());


 //Create Slice Manager
 SliceManager sliceManager = new SliceManager(sliceID, <sliceserializer>, new FileStorageManager(<slicesdirectory>));

 //Read Graph Partition
 IPartition graphPartition = sliceManager.readPartition();

 //Read Graph Instances for all the vertex and edge properties
 ISubgraphInstance graphInstances = null;
 for(ISubgraph subgraph : graphPartition){
    graphInstances = sliceManager.readInstances(subgraph, <startrange>, <endrange>, subgraph.getVertexProperties(), subgraph.getEdgeProperties());
 }


 ***************
 Jython Library
 ***************

 a) Install Jython

  Below link explains about the installation of Jython
  http://wiki.python.org/jython/InstallationInstructions

 b) Add the gofs-core-1.0.jar and dependent jars in sys.path of jython script

 c) Import the 'edu.usc.goffish.gofs.partition.gml', 'edu.usc.goffish.gofs.slice', 'edu.usc.goffish.gofs.namenode' and other necessary java packages in the script

 d) Use ISliceManager to read graph instances from disk. FileNameNode should be used to read the slice id of partition.
 Read Graph Instances(After Graph Deployment):
 Code Snippet:

 //Read the Partition Metadata Slice Id
 remoteNameNode = new RemoteNameNode(<host>, <port>);
 uri = remoteNameNode.getPartitionMapping(graphId, partitionId);
 sliceID = UUID.fromString(uri.getFragment());


 //Create Slice Manager
 sliceManager = new SliceManager(sliceID, <sliceserializer>, new FileStorageManager(<slicesdirectory>));

 //Read Graph Partition
 graphPartition = sliceManager.readPartition();

 //Read Graph Instances for all the vertex and edge properties
 for(ISubgraph subgraph : graphPartition){
    graphInstances = sliceManager.readInstances(subgraph, <startrange>, <endrange>, subgraph.getVertexProperties(), subgraph.getEdgeProperties());
 }


 Note: Refer Java documentation for exact argument types