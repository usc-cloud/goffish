Vector Comparison 

The goal of this application is to calculate Pearson Correlation (based on Apache Commons Mathematics Library) on all pairs of input vectors. 

In the code, it is assumed that the graph has four partitions, and they are distributed on four different machines where containers are deployed.

Building Application
* You can build the application using $mvn clean install command which will create vect-comp-0.9-SNAPSHOT.jar
* Install application in floe (see Readme.pdf)

Deploying the workflow
* Once floe is running, you can deploy the workflow defined in vector-comparison/graph/vect-comp.xml in floe by running edu.usc.pgroup.goffish.gopher.sample.client.GraphStart in the vector-comparison source folder we have provided. Pass command line arguments : coordinator Host name and vect-comp.xml path 
* For a sample output, please refer to Readme.pdf
* Then run the edu.usc.pgroup.goffish.gopher.sample.client.Client with command line arguments HostName to connect , data port and control port which will initiate the workflow. 
* After the computation, some .txt files would be generated at containers. For each line in these .txt files should have the following format  

X,Y	Z, where X and Y refer to the compared vectors and Z stands for the Pearson Correlation between these two vectors	

