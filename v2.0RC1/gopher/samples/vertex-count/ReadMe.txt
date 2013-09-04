Distributed Vertex Count application which counts total number of vertices in a Large Graph.*8
--------------------------------------------------------------------------------------------

*Each subgraph processor calulate number of vertices within a subgraph. 

*They send messages to all other subgraphs with their count where each subgraph caluculate the total 
using the messages received. 

*Each processor writes the total to the disk.


How to run.

Use GopherClient.sh to start the application.
Example :
$ ./GopherClient.sh ~/projects/USC/goffish-test/GoFFish/gopher-client-2.0/gopher-config.xml ~/projects/USC/goffish-test/gofs-2.0/conf/gofs.config 1 vert-count-2.0.jar edu.usc.pgroup.goffish.gopher.sample.VertCounter NILL
