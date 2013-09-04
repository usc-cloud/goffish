********************
Sample Application
********************

Jython script to write/read graph partition and instances. It also does BFS traversal on the subgraphs and prints all the vertex attribute values, and also computes the total edge weight.

This script will give initial idea about accessing GoFS API through Jython.

**********
  Usage
**********

a) Assuming Jython is installed on the system. If not, Below link explains about the installation of Jython
   http://wiki.python.org/jython/InstallationInstructions

b) Script assumes that the dependent jars are under ./gofs-core-0.9.jar and ./lib/*.jar directory. Edit the sys.path statements in the script to specify the absolute path of these jars

c) Execute the jython script
   % jython bfs.py

