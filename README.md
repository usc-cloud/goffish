Overview
--------

Sensors and online instruments performing high fidelity observations are contributing in a large measure to the growing big data analytics challenge. These datasets are unique in that they represent events, observations and activities that are related to each other while being recorded by independent data streams. GoFFish (Graph-Oriented Framework for Foresight and Insight using Scalable Heuristics) is a scalable graph-oriented analytics framework well suited for processing reservoirs of interconnected distributed data fed by event data generators. It minimizes the communication overhead by grouping together tightly bind data.
 
![GoFFish concept](http://losangeles.usc.edu/usc-cloud/goffish/GoFFish.png "GoFFish concept.")

A printable executive summary can be found [here](GoFFish-exec-summary.pdf)

Objectives
----------

**Efficiently store interconnected data by using a specialized graph-oriented file system**:

*	Support for widely used graph formats such as GML
*	Take advantage of the various graph information and layout to enable efficient data loading
*	Facilitate storing temporal data to enable analytics on evolving graphs 
*	Enable distributed information storage in-line with current trends observed in cloud systems

**Efficiently compose graph analytics on large datasets by grouping data together**:
*	Adopt a sub-graph centric approach which performs more computations locally and reduces communication overhead
*	Offer high level programming constructs at sub-graph level that hide low level graph details from the developer
*	Customizable output format that can be mapped to any format needed by visualization tools

**Enable fast analytics on certain classes of analytics and graphs**:

*	Performance improvement for analytics that require sub-graph knowledge, e.g., community analytics, high impact nodes, shortest paths
*	Suited for graphs that can be partitioned evenly across processors with minimal communication between them, e.g., sparse graphs (road networks), Internet/network graphs

Benefits
--------

GoFFish offers several key benefits to developers…
*	Conversion pipeline from widely used graph formats such as GML
*	Specialized graph-oriented file system to easily store interconnected evolving data
*	High level API for composing graph analytics on static and evolving graphs
*	Easily customizable output for interfacing with various visualization tools

…and to analysts:

*	Support for multiple graphs and analytics
*	Drive analytics on evolving graphs 
*	Ability to incorporate new analytics 

Specifications
--------------

GoFFish is designed as a layered architecture with two main components: the GoFS graph file system and the Gopher analytics abstraction on top of it. 


**GoFS**


A conversion pipeline from the GraphML format to the GoFS file format allows easy conversion of any graph. The conversion process relies on a graph partitioning stage at which point the graph is split into sub-graphs to balance its subsequent execution on distributed cloud infrastructures.

GoFS is very versatile with respect to the graphs it can integrate. Numerous graphs ranging from road networks to social networks have been successfully converted. 

**Gopher**

GoFFish uses a high level API to intuitively and rapidly compose graph and event analytical models. The composed application enhances data parallel analytics beyond the traditional Map Reduce models using a novel distributed data partitioning approach based on edge distance heuristics. This allows unprecedented insight from the reservoirs of evolving data for commanders to perform causal graph analysis and strategic planning. 

Measures of effectiveness
-------------------------

We evaluated our platform on a configuration representative for both clouds and commodity clusters. The system comprised of cluster of 12 nodes, each with an 8-core Intel Xeon CPU, 16 GB RAM, 1 TB SATA HDD, and connected by Gigabit Ethernet. We have compared the platform against the main competitor, Apache Giraph. Both systems were installed using Java 7 JRE for 64 bit Ubuntu Linux. The datasets consisted of three real world graphs: California road network (1.6M nodes x 2.7M edges), a network trace route (20M nodes x 23M edges), and the Live Journal social network (5M nodes x 65M edges). Different graph analytics such as connected components, shortest path and page rank were deployed on it and their speed-ups were measured. An average improvement of 10x was observed.

Required Skill Sets
-------------------

* To use the framework on a given data set:

 * Required

   * Familiarity with Linux

   * Manipulating graph data (potentially to convert the given data to GML format)

   * Java programming skills

 * Good to have

   * Familiarity with Virtual Box and virtualization to quickly deploy using the quick guide

   * Experience with the BSP programming model

 * To setup the environment on a cluster

   * Cluster administration knowledge

   * Linux cluster administration skills

How to get it
---------------

The framework is hosted in the GitHub repository at https://github.com/usc-cloud/goffish

Clone the repository using:

<code>git clone https://github.com/usc-cloud/goffish.git</code>

Note: You may need to install a git client to download the repository.

Current source code is located in the *goffish/goffish-trunk* directory

Installation
------------

A quick start guide can be found [here](QuickStart.md) together with a precompiled VM to help you get started.

GoFFish consists of a Graph File System (GoFS) which can be used as a standalone product. Detailed deployment documentation can be found [here](goffish-trunk/gofs/docs/README.md)

Gopher is the distributed subgraph centric programming framework of GoFFish. Gopher programming API overview and an example can be found [here](goffish-trunk/gopher/docs/GopherAPIGuide.md). Deployment details of Gopher can be found [here] (goffish-trunk/gopher/docs/GopherdeploymentGuide.pdf) 

User and Development Discussions
--------------------------------
All GoFFish development discussions and user discussions were movied to an open forum. If you have any questions regarding GoFFish or if you want to contribute; Please join our discussion forum [here] (http://groups.google.com/d/forum/usc-cloud)


Future enhancements
-------------------

Numerous enhancements are in progress, most of them related to dealing with detecting online events that can occur in evolving streaming graphs. The loop between insight and foresight will be closed by coupling event patterns mined from historical stream reservoirs with graph analytics based on real-time event streams from sensors.


