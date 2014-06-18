<H1>Quick Start GoFFish - A Subgraph Centric Framework for Large Scale Time SeriesGraph Processing</H1>

<H2>Subgraph centric programming model</H2>
<p>
Vertex centric programming model getting populer for large scale graph processing due to simplicity of the programming model and public avaiablity of software frameworks like <a href="https://giraph.apache.org/">Apache Giraph</a>. But vertex centric programming model have its own down sides mainly due to the slow convergence of vertex centric algorithms and high communication overhead. 

Vertex centric programming model force programmer to think like a vertex when developing algorithms. Logic is executed at each vertex in the graph in parallel. Program can communicate with other vertices using edges and vertex ids. 

Subgraph centric programming model can be thought as an extention to vertex centric programming model where programer logic is executing at subgraph level. In this model graph is initially partitioned and distributed accross multiple computing nodes and subgraphs (or weakly connected components) were identified. Computation within the subgraph happen in memory where subgraphs can communicate with each other using messages. </p>
<br>
This document guide you to run a simple <a href="https://github.com/usc-cloud/goffish/blob/master/goffish-trunk/gopher/samples/vertex-count/src/main/java/edu/usc/pgroup/goffish/gopher/sample/VertCounter.java">subgraph centric algorithm</a> to get total number of vertices in the graph in your local machine. 

<H2>Quick Start With Development VM</H2>

Pre installed virtual machine is used in this guide which can be installed in your local machine. Following installation instructions assume a linux operating system environment.	

<H3>Download Virtual Machine</H3>
<p> Download the pre installed virtual machine from <a href="http://losangeles.usc.edu/usc-cloud/goffish/goffis_vm.zip">here</a>
</p>

<H3>Setting up</H3>
<p>
Install Oracle Virtual Box 4.3.8. You can download it from <a href="https://www.virtualbox.org/wiki/Download_Old_Builds_4_3">here</a>.
<br>
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

<H4>Setting up GoFS (GoFFish graph file system)</H4>
<p>
Log in to the virtual machine
<br>
<code>vagrant ssh</code><br>
</p>
<p>
Change the password of root user and changed to root user<br>
<code>$sudo passwd root</code><br>
<code>$su root</code><br>
</p><p>
Move GoFS and and Gopher Server to goffish_home directroy.<br> 
<code>$mv gopher-server-2.0 goffish_home/</code><br>
<code>$mv gofs-2.0/ goffish_home/</code><br>
</p><p>
Start the name node.A name node is an interface that tracks all the metadata for a particular GoFS installation. At the moment GoFS ships with a REST server based namenode, which can be run through the GoFSNameNode script. This script accepts a URI argument and attempts to start a name node REST server at this URI. Optionally a file may also be specified which is used to save name node state. It is recommended you always specify a save file or name node state may be lost if the process is killed. Once a name node exists it is referenced through a java class type and a URI. The default type is edu.usc.goffish.gofs.namenode.RemoteNameNode which can communicate with a remote REST server based name node. <br>
</p><p>
<code>cd $GOFFISH_HOME/gofs-2.0/bin</code><br>
<code>./GoFSNameNode http://localhost:9998</code><br>
</p><p>
Login to vm from another terminal<br> 
<code>$vagrant ssh</code><br>
</p><p>
Change to root<br>
<code>$su root</code><br>
</p><p>
Create directory named gofs-data in GOFFISH_HOME. This will be used to store the graph data in binery format. <br>
<code>$mkdir $GOFFISH_HOME/gofs-data</code><br>
</p><p>
Edit gofs.config located at /home/vagrant/deployment/goffish_home/gofs-2.0/conf . Change the datanode path to  
<code>gofs.datanode = file://localhost/home/vagrant/deployment/goffish_home/gofs-data/ </code> <br>
Set name node url property in gofs.config to <code>http://localhost:9998</code> <br>
</p>
<p>
Change in to GoFS binery directroy and and format the graph file system.<br> 
<code>$cd $GOFFISH_HOME/gofs-2.0/bin</code><br>
<code>$./GoFSFormat</code><br>
</p>
<p>
A Sample graph which can be used is located at /home/vagrant/deployment/samples/gofs-samples directory. To deploy this graph in the GoFS file system we need to create a list of graph and graph instances. In this example we will be only using graph template which is equalent to a static graph.

Create a file named list.xml in the /home/vagrant/deployment/samples/gofs-samples directroy
</p> <p>
<code>
<gml>
 <template>/home/vagrant/deployment/samples/gofs-samples/Template.gml</template>
</gml>
</code>

</p><p>
Deploy the graph. This will partition the graph convert partitions into binery format and copy the files into graph storage directory. In this case we will only have a single graph partition.<br>
<code>$cd $GOFFISH_HOME/gofs-2.0/bin</code><br>
<code>$./GoFSDeployGraph edu.usc.goffish.gofs.namenode.RemoteNameNode http://localhost:9998 "graph1" 1 /home/vagrant/deployment/samples/gofs-samples/list.xml</code><br>
</p>

Now the sample graph is deployed in the GoFS file system.

<H4>Setting up and Running applications</H4>
<p>
To start and run Gopher goto Gopher client directory and deploy the vertex count application.<br>
<code>$cd /home/vagrant/deployment/gopher-client-2.0/bin</code><br>
<code>$./CopyArtifacts.sh /home/vagrant/deployment/samples/vert-count-2.0.jar vert-count-2.0.jar root localhost</code><br>
</p>
<p>
Start the servers. To start the servers we need to have the location lists in compute nodes.<br> 
<code>$./ListDataNodes.sh  /home/vagrant/deployment/goffish_home/gofs-2.0/conf/gofs.config</code><br>

Above command will list the deployment directory locations in the distributed graph file system. Use that locations to startup Gopher system.<br>


<code>$./StartServers.sh root localhost localhost "graph1" file://localhost/home/vagrant/deployment/goffish_home/gofs-data/gofs</code> <br>
<code>$./DeployGopher.sh /home/vagrant/deployment/goffish_home/gofs-2.0/conf/gofs.config localhost localhost</code><br>
</p>
<p>
Now Every thing is setup and ready to run gopher applications. You can run gopher applications using GopherClient command.<br>
<code>$./GopherClient.sh /home/vagrant/deployment/gopher-client-2.0/gopher-config.xml /home/vagrant/deployment/goffish_home/gofs-2.0/conf/gofs.config graph1 vert-count-2.0.jar edu.usc.pgroup.goffish.gopher.sample.VertCounter NILL</code><br>
</p><p>
Executing this algorithm will output the vertex count of the input graph. You can find the results in $GOFFISH_HOME/gopher-server-2.0/vert-count.txt
</p>
<p>
Mode detailed informations regarding general deployment of <a href="https://github.com/usc-cloud/goffish/tree/master/goffish-trunk/gofs/docs">GoFS</a> and <a href="https://github.com/usc-cloud/goffish/tree/master/goffish-trunk/gopher/docs">Gopher</a> is avaialbe in github.
</p>
Publications: <br>

 <a href="http://arxiv.org/abs/1311.5949">GoFFish: A Sub-Graph Centric Framework for Large-Scale Graph Analytics</a>, 
Yogesh Simmhan, Alok Kumbhare, Charith Wickramaarachchi, Soonil Nagarkar, Santosh Ravi, Cauligi Raghavendra and Viktor Prasanna,
European Conference on Parallel Processing (Euro-Par) , 2014

</p>

