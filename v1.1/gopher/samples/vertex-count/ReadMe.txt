Distributed Vertex Count application which counts total number of vertices in a Large Graph.*8
--------------------------------------------------------------------------------------------

*Each subgraph processor calulate number of vertices within a subgraph. 

*They send messages to all other subgraphs with their count where each subgraph caluculate the total 
using the messages received. 

*Each processor writes the total to the disk. 