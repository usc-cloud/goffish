Distributed Connected component application which lables Connected components in a graph 
--------------------------------------------------------------------------------------------

*Each subgraph finds the smallest vertex id for each subgraph and propagate that smallest value to its connected subgraphs. If incoming value to a subgraph is different from its current value it updates the current value and propagate the changes to its neighbours. 


How to run.

Use GopherClient.sh to start the application.
Example :
$ ./GopherClient.sh ~/gnutella-gofs-setup/gopher-client-2.0/gopher-config.xml ~/gnutella-gofs-setup/gofs-2.0/conf/gofs.config 1 connected-components-1.0.jar edu.usc.goffish.gopher.sample.ConnectedComponents NIL
