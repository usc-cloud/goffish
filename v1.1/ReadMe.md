GOFFish  Documentation
----------------------
GOFFish is a Graph oriented Framework for time series big data. GoFFish 0.9 mainly consist of two components

GoFS - A Graph oriended File system for time series graphs
Gopher - A Subgraph centric analytics framework for time series graphs. 

GoFS documentation 
-------------------
1)  gofs/framework/GoFS Archtecture.pdf   describes the Highlevel GoFS architecture. 
2)  gofs/framework/ReadMe.md   privdes the installation, deployment and execution instructions for GoFS


Gopher documentation 
--------------------
Gopher is a BPS work flow application deployed on top of Floe. Detailed descripion of floe can be found at 
1) http://ganges.usc.edu/wiki/Floe

Instrctions on installation and running Gopher samples can be found in 
2) gopher/framework/ReadMe.pdf (or ReadMe.md)


Website
-------
GoFFish comes from the University of Southern California, and released under the Apache 2.0 License. 
You can find more details on GoFFish and Cloud computing research activities at: http://ganges.usc.edu/wiki/Cloud_Computing
GoFFish can be downloaded from the GitHub project page at: https://github.com/usc-cloud

TOC
---

├───datasets - sample graph template and instance files in GML format
├───floe  - required streaming engine (prereq for gopher)
├───gofs  - distributed file systems for storing and accessing time series graphs (prereq for gopher)
│   └───samples - sample jython script to access data from gofs
├───gopher - Distributed subgraph centric pregel/bsp framework for graph analytics
│   └───samples 
│       └───vertex-count - sample gopher application to count total number of vertices in the distributed subgraphs
└───utils
    └───gefi-viz-plugin - gefi plugin to visualize time series graph (in extended gml format)
