Road network traffic graph generator will generate Time series graph on top of a given graph topology simulating 
vehical movements.

HOW TO BUILD : 
--------------------------------

run the command $mvn clean install 

RUNING Graph Generator 
--------------------------------

*Configure the generator.properties file.

Following are the configuration parameters : 

	topology-path to the graph topology file
	dir=Directory where generated GML files will be stored
	prefix=Prefix of the generated gml files
	instance-count=number of instances to generate

*copy graph-gen-0.9-SNAPSHOT.jar to the dir where generator.properties file is stored. 

*Execute by running the command $java -cp graph-gen-0.9-SNAPSHOT.jar edu.usc.pgroup.graph.GraphGenerator

This will generate GML files of the time series grap in the configred directory. 






