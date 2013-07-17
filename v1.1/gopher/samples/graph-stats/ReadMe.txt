Distributed Graph Statistics application
----------------------------------------

This calculates statistics for different sub-graphs, partitions and the entire graph in 3 supersteps.
These stats include # of vertices, edges, remote vertices, edge degree, and subgraphs.
Both aggregates and a histogram are collected. 
Depending on verbosity (0, 1, 2, 3), these details are saves to files.

In step 0, details of subgraphs are collected. These are send as a message to the "same" local partition. 
If verbosity >=3 details for each subgraph are saved to file.

In step 1, details across subgraphs are collected by the partition. 
One of the subgraph plays the role of aggregator.
This aggregator subgraph depends on which subgraph is able to create a file for that partition.
The aggregates are sent as a message to a single partition with the smallest partition number. 
If verbosity >=2 details for each partition are saved to file.

In step 2, details across partitions are collected by the partition. 
One of the partitions (with the smallest ID) plays the role of aggregator.
If verbosity >=1 details for the graph are saved to file.

