import sys
import os.path
sys.path.append(os.path.realpath('gofs-core-1.0.jar'))
sys.path.append(os.path.realpath('lib\jersey-core-1.17.1.jar'))
sys.path.append(os.path.realpath('lib\jersey-client-1.17.1.jar'))
sys.path.append(os.path.realpath('lib\jersey-server-1.17.1.jar'))
from collections import deque
from edu.usc.goffish.gofs.partition.gml import *
from edu.usc.goffish.gofs.slice import *
from edu.usc.goffish.gofs.namenode import *
from java.io import *
from java.lang import *
from java.nio.file import *
from java.util import *
from java.lang import *
from java.net import *
from javax.ws.rs.core import *

def __getGraphInstancePathList(graphInstances):
	graphInstancesPathList = ArrayList(len(graphInstances))
	for instance in graphInstances:
		if instance is not None:
			if(not os.path.isfile(str(instance))):
				raise RuntimeError("File does not exist in the specified path: " + str(instance))
			graphInstancesPathList.add(Paths.get(File(str(instance)).toURI()))

	return graphInstancesPathList

def __printBFS(subgraph, graphInstances):
	vertices = subgraph.getTemplate().vertices()
	visitedNodes = []
	queue = deque([])
	edgeWeight = 0

	#Subgraph Instances
	for instance in graphInstances:
		#Read the start vertex
		vIterator = vertices.iterator()
		if vIterator.hasNext():
			startVertex = vIterator.next()

		#Add the start node
		queue.append(startVertex)
		
		while len(queue) > 0:
			vertex = queue.popleft()
			visitedNodes.append(vertex.getId())
			print str(vertex.getId()) + " \n\t---> " + instance.getPropertiesForVertex(vertex.getId()).getValue("license-list")

			for edge in vertex.outEdges():
				edgeWeight += instance.getPropertiesForEdge(edge.getId()).getValue("weight")
				sinkVertex = edge.getSink()

				if visitedNodes.count(sinkVertex.getId()) <= 0:
					queue.append(sinkVertex)
					visitedNodes.append(sinkVertex.getId())

	print "\nTotal Edge Weight: " + str(edgeWeight)


# Main method
if __name__ == "__main__":
	#GMLPartition object
	graphInstances = ["Instance_0.gml"]
	gmlPartition = GMLPartition.parseGML(long(1234), FileInputStream("Template.gml"), GMLFileIterable(__getGraphInstancePathList(graphInstances)))

	#Slice Manager
	slicesPath = Paths.get("slices")
	if not Files.exists(slicesPath):
		Files.createDirectory(slicesPath)

	#Create FileNameNode to put the mapping of partition id and partition slice id
	#In application deployment, slices will be written from the name node. So, partition slice id will not be available to the client
	#Therefore, slice id can be obtained from fragments parameter of URI 
	fileNameNode = FileNameNode(slicesPath.resolve("filenamenode"))
	sliceID = UUID.randomUUID()
	fileNameNode.putPartitionMapping("1", long(1234), UriBuilder.fromUri("http://localhost:80").fragment(sliceID.toString()).build())

	##################################################################
		# Write Graph Partition and Graph Instances
	##################################################################

	#Create slice manager to write the template and instances. In real-time usecase, Write will be done from the name node
	writeSliceManager = SliceManager(sliceID, JavaSliceSerializer(), FileStorageManager(slicesPath))

	#Write Partition
	writeSliceManager.writeTemplate(gmlPartition)

	#Write Instances
	writeSliceManager.writeInstances(gmlPartition)

	##################################################################
		# Read Graph Partition and Graph Instances
	##################################################################

	#Create slicemanager for the partition by reading the slice id from file name node. Trying to simulate the real-time usecase
	readSliceManager = SliceManager(UUID.fromString(fileNameNode.getPartitionMapping("1", long(1234)).getFragment()), JavaSliceSerializer(), FileStorageManager(slicesPath))
	#Read Partition 
	actualPartition = readSliceManager.readPartition()
	subgraph = actualPartition.getSubgraph(34)

	#Read Instances
	graphInstances = readSliceManager.readInstances(subgraph, Long.MIN_VALUE, Long.MAX_VALUE, subgraph.getVertexProperties(), subgraph.getEdgeProperties())

	#BFS Print
	__printBFS(subgraph, graphInstances)