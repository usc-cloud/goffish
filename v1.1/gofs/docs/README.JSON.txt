TEMPLATE GRAPH
==============
- "directed": Set to true if the graph is directed. false if it is undirected.

- "node_properties": List of objects, each describing a property that can be present in the vertices of the graph. Properties that are not listed here cannot be present in the instances. The property names and types are common across all vertices in the graph. Note that "id" is a reserved property name of type signed long int (2^(8*8)) and is_static=true present in all vertices.
- "edge_properties": List of objects, each describing a property that can be present in the edges of the graph. Properties that are not listed here cannot be present in the instances. The property names and types are common across all edges in the graph. Note that "id", "source" and "target" are  reserved property names of type signed long int (2^(8*8)) and is_static=true present in all edges.
-- property_name (e.g. "label")
--- "is_static": true, if the value for this property is a constant and does not change in instances, but can be different for each unique vertex/edge in the template. false if the values can change in instances. Note that when static is false, the vertices can still be assigned a default value in the template that can be optionally overridden in the instances.
--- "type": property type, 'string', 'long', 'int', 'float', 'double' or 'boolean'. For now, support for lists is not native (but planned). Users can encode the list within a string  (e.g. as comma separated value). Similarly, support for user-defined enums is not native. Users can encode these as ints.

- "nodes": Identifies a unique vertex in the template graph with it associated static and default values.
-- "id": The vertex is identified by an "id" property that is unique among all vertices in the template. The id property must be present for every vertex. It is static. Int and double property values such as for "id" should not be quoted. String values need to be quoted.
-- property_name: One of the properties specified in the "node_properties" object. If the node has static properties, it must be present along with an associated value that will be constant across instances. Otherwise, it is optional to have the property listed for the node, unless a default value need to be provided for the property. e.g. "label" is a static property whose presence is required.

- "edges": Identifies a unique edge in the template graph with it associated static and default values. If edges are undirected, then only one pair of source:target vertex ids can be present, irrespective on which position they appear (e.g. either 20:35 or 35:20 can appear once). If they are directed, there can only be only one unique source:target vertex ids in those positions (e.g. 20:35 and 35:20 can each appear once).
-- "id": The edge is identified by an "id" property that is unique among all edges in the template. The id property must be present for every edge. It is static. 
-- "source": The id property of the vertex that is the source vertex for this edge. There should be a corresponding vertex with this id value. If the graph is undirected, then the source and target are interchangeable. The source property must be present for every edge. It is static. 
-- "target": The id property of the vertex that is the sink vertex for this edge. There should be a corresponding vertex with this id value. If the graph is undirected, then the source and target are interchangeable. The target property must be present for every edge. It is static. 
-- property_name: One of the properties specified in the "edge_properties" object. e.g. "distance_miles" is a static property, "lanes" is a non-static default property that can be overriden in an instance, while "speed" is a non-static property without a default value.

