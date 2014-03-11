This tool is used to generate GML template and instance files from SNAP or DIMACS graph format.

USAGE: GMLGenerator -type <dataset/type> -input <dataset/file/name> -directed <directed> -properties <config/properties> -instances <no/of/instances> -out <output/directory>

<dataset/type> - Specify either SNAP or DIMACS format
<dataset/file/name> - File path of input graph
<directed> - Specify true, if the graph is directed
<config/properties> - Config file to specify vertex and edge properties
<no/of/instances> - Specify the number of instances to be generated
<output/directory> - Specify the directory to generate GML files

You can specify the vertex and edge properties in config file. Supported format in config file:
vertex,property-name,is-static,property-type
edge,property-name,is-static,property-type

For Example:
vertex,property-1,true,integer
edge,property-1,false,string

Only "integer", "string" and "float" types are supported.



