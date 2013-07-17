This is a Simple application of tracing a route of a car using time series graphs. 

Building application
* You can build the application using $mvn clean install command which will create  car-tracing-0.9-SNAPSHOT.jar
* Install application in floe(see Gopher readme)
Deploying the workflow
* Once floe Is running you can deploy the workflow defined in graph/car-tracing-single.xml in floe by running edu.usc.pgroup.goffish.gopher.sample.client.GraphStart 
in the car-tracing source we have provided. Pass command line arguments : coordinator Host name and car-tracing-single.xml path. Include the floe depdendcy libs , 
gofs and its depedency libs and gopher to the class path when running the application. If you create a IDE project using maven it will add this depedencies for you. 
  
* You will get an output like follows which gives connection details to send the workflow initializing message.
********************************************
Channel Info
 Connection Info Details
hostAddress = 68.181.17.11 (HostName to connect)
tcpListenerPort = 65446 (Data Port)
Control Channel info
 Connection Info Details
hostAddress = 68.181.17.11 (HostName to connect)
tcpListenerPort = 65448  (Control Port)
********************************************
* Then run the edu.usc.pgroup.goffish.gopher.sample.client.Client with command line arguments args[0]=Host to connect  ,  arg[1]=data port arg[2]=control port , arg[3]=licenceID,arg[4]=lastknown time in mills  which will initiate the workflow.
* After the end of execution each container will have a file named path.txt which will contain trace of the given car.   

NOTE : This application is only tested in a psudo distributed setup. with single partition. 

Contact Charith Wickramaarachchi cwickram@usc.edu for more infomation. 