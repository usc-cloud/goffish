<H1>Quick start guide for parallel Louvain method for community detection</H1>

<H2>Community Detection In Graphs</H2>
<p>
Interactions in online social networks, communication networks can be naturally modeled as graphs. Structural communities in graphs generally defined as groups of highly connected vertices/nodes in the graph. This groups generally represent similar interest groups, friend communities etc.  
<br>
Finding communities in large graphs is a very useful technique when trying to narrow down analytics to smaller targeted groups.This can be used for online marketing, online surveillance etc. 
<br>
Detecting communities in graphs is computationally very expensive, specially for large graphs.
</p>

<img src="http://farm8.staticflickr.com/7100/7046439385_b83413587a_b.jpg" height="500" width="500"/>

<H2>Parallel Algorithm For Fast Community Detection</H2>
<p>
This document provides a quick guide to setting up and running our parallel implementation for <a href="https://sites.google.com/site/findcommunities/">Louvain community detection method</a> to detect non overlapping communities in large graphs . 
<br>
Lovain community detection method detect communities in a graph in a hierarchical manner. At each level detected communities are reduced to vertices, reducing the graph size. This enable zoom in and out of high level communities. 
</p>

<img src="https://sites.google.com/site/findcommunities/pol.jpg" height="300" width="500"/>
<p>Detailed description of Parallel Louvain method can be found <a href="https://xd-web-proxy.data-tactics-corp.com/wiki/display/ANL/GoFFish+Subgraph+Oriented+Graph+Analytics+Experiments">here</a>.</p>

<H2>Quick Start With Development VM</H2>

Our distributed memory parallel implementation of Louvain community detection based on MPI 2.0 runtime environemnt. Following instrcutions guide you through how to run a this on a sample graph to detect communities.

Pre installed virtual machine is used in this guide which can be installed in your local machine. 

<H3>Download Virtual Machine</H3>
<p> Download the pre installed virtual machine from <a href="http://losangeles.usc.edu/usc-cloud/goffish/parallel-louvain-mpi.zip">here</a>
</p>

<H3>Setting up</H3>
<p>
Install Oracle Virtual Box 4.3.8. You can download it from <a href="https://www.virtualbox.org/wiki/Download_Old_Builds_4_3">here</a>.

Install and configure Vagrant<br>
<code>sudo apt-get install vagrant</code>
<br>
Add Vagrant plugin that keeps Virtual Box Guest Additions in sync.<br>
<code>vagrant plugin install vagrant-vbguest</code>
<br>
Extract the downloaded virtual machine<br>

Go to the virtual machine directory and start the environment<br>
<code>vagrant up</code><br>
<br>
After it boots, log in to the VM<br>
<code>vagrant ssh</code><br>
<br>
Other useful vagrant commands:<br>
<code>vagrant suspend</code><br>
Saves the current running state of the machine and stops it. vagrant up will resume.

<code>vagrant halt</code><br>
Gracefully shuts down the vm. vagrant up will boot it back up.
<br>
<code>vagrant destroy</code>
<br>
Destroys the vm (and all the cruft inside of it). vagrant up will reprovision and run the deploy scripts again.
<br>
More info at http://www.vagrantup.com/
</p>

<H3>Running The Sample</H3>
<p>
Log in to the virtual machine
<code>vagrant ssh</code><br>

Source code is and sample data sets are located in /vagrant/code/louvain-mpi directory.
<code>cd /vagrant/code/louvain-mpi</code><br>

A Sample small graph is located in this directory.
*4elt.txt - graph in edge list format. 
*4elt.graph - graph in metis graph format. 
<br>
First step of execution is partitioning the graph and creating the graph partitions for parallel louvain algorithm. This is done using converter-script.sh
<code>./converter-script.sh</code><br>

Executing this will compile the source code, partition the graph into 2 partitions and create graph partitions.

<code>
Cleaning up<br>
...<br>
Compiling the source code<br>
make<br>
mpic++ -fopenmp -o graph_binary_better.o -c graph_binary_better.cpp -ansi -O5 -Wall<br>
mpic++ -fopenmp -o graph_binary.o -c graph_binary.cpp -ansi -O5 -Wall<br>
....<br>
Done compilation...<br>

---------------------------------------<br>
4elt.txt : Graph in louain format<br>
4elt.graph : Graph in metis format<br>
---------------------------------------<br>
Partitioning the graph...<br>
.....<br>
$pmetis 4elt.graph 2<br>
**********************************************************************<br>
  METIS 4.0.1 Copyright 1998, Regents of the University of Minnesota<br>

Graph Information ---------------------------------------------------<br>
  Name: 4elt.graph, #Vertices: 15606, #Edges: 45878, #Parts: 2<br>

Recursive Partitioning... -------------------------------------------<br>
  2-way Edge-Cut:     142, Balance:  1.00 <br>

Timing Information --------------------------------------------------<br>
  I/O:          		   0.000<br>
  Partitioning: 		   0.010   (PMETIS time)<br>
  Total:        		   0.010<br>
**********************************************************************<br>

Converting Graph to Binery format...<br>
./convert -i 4elt.txt -o 4elt -p 4elt.graph.part.2 -n 2
<br>
...<br>
Done converting to binery..<br>

</code>

Executing this will compile the source code, partition the graph into 2 partitions and create graph partitions.<br>
To Run the the parallel louvain algorithm run the run-louvain.sh script <br>
<code>./run-louvain.sh</code>

Executing this script with run the algorithm in two MPI processors and output the community graph at 2nd level of louvain method to level2.txt 
<br>
level2.txt contains the graph in adjacency list format.
each line is in following format
source: (sink1 weight1) (sink2 weight2) … (sinkN weightN)
<br>
Example:
<code>
0: (0 14) (1 1) (23 3) (33 4) (128 1) (134 2) (183 3) (3037 2) (3038 2) (3078 1)
1: (0 1) (1 14) (6 3) (33 4) (48 3) (74 1) (3037 1) (3053 3)
2: (2 14) (3 3) (5 3) (8 1) (9 3) (3106 2) (3108 3) (3110 1)
3: (2 3) (3 10) (4 3) (7 1) (8 3) (3110 4)
4: (3 3) (4 6) (7 3) (8 2) (10 3) (13 1)
5: (2 3) (5 10) (9 1) (12 3) (16 2) (19 1) (3108 2) (3111 3)
6: (1 3) (6 10) (21 3) (48 3) (86 1) (222 2) (3053 3)
7: (3 1) (4 3) (7 18) (13 5) (3110 1) (3112 4)
8: (2 1) (3 3) (4 2) (8 10) (9 3) (10 1) (11 3)
9: (2 3) (5 1) (8 3) (9 6) (11 1) (15 1) (16 3)
10: (4 3) (8 1) (10 6) (11 3) (13 2) (14 3)
11: (8 3) (9 1) (10 3) (11 6) (14 1) (15 3) (27 1)
12: (5 3) (12 10) (17 3) (19 3) (31 1) (3111 1) (3120 3)
13: (4 1) (7 5) (10 2) (13 14) (14 3) (18 3)
14: (10 3) (11 1) (13 3) (14 10) (18 1) (20 3) (27 3)
15: (9 1) (11 3) (15 6) (16 3) (27 3) (29 2)
16: (5 2) (9 3) (15 3) (16 10) (19 4) (29 1)
17: (12 3) (17 6) (24 2) (31 3) (51 2) (72 3) (3120 1)
</code>

</p>

