// Copyright (C) 2006 The Trustees of Indiana University.

// Use, modification and distribution is subject to the Boost Software
// License, Version 1.0. (See accompanying file LICENSE_1_0.txt or copy at
// http://www.boost.org/LICENSE_1_0.txt)

//  Authors: Nick Edmonds
//           Andrew Lumsdaine



#include <boost/graph/use_mpi.hpp>

//#include <boost/graph/distributed/betweenness_centrality.hpp>
#include "chong_extract_high_betweenness.hpp"

#include <boost/graph/distributed/adjacency_list.hpp>

#include <boost/config.hpp>
#include <boost/throw_exception.hpp>
#include <boost/graph/distributed/mpi_process_group.hpp>
#include <boost/graph/distributed/concepts.hpp>
#include <boost/graph/erdos_renyi_generator.hpp>

#include <boost/random/linear_congruential.hpp>
#include <boost/graph/graphviz.hpp>
#include <boost/property_map/vector_property_map.hpp>

// METIS Input
#include <boost/graph/metis.hpp>
#include <boost/graph/distributed/graphviz.hpp>

//#include <cstring>
#include <sys/time.h>

#ifdef BOOST_NO_EXCEPTIONS
void
boost::throw_exception(std::exception const& ex)
{
	std::cout << ex.what() << std::endl;
	abort();
}
#endif

using namespace boost;
using boost::graph::distributed::mpi_process_group;

char* out_file;

int main(int argc, char* argv[])
{
	mpi::environment env(argc, argv);

	typedef adjacency_list<vecS,
	        distributedS<mpi_process_group, vecS>,
	        undirectedS,
	        property<vertex_centrality_t,float>,
	        property<edge_weight_t, int> > Graph;

	typedef graph_traits<Graph>::vertices_size_type vertices_size_type;
	vertices_size_type extract_count = std::atoi(argv[1]);
	vertices_size_type max_stable_count = std::atoi(argv[2]);
	vertices_size_type batch_count = std::atoi(argv[3]);
	
	const char* filename = argv[4];
	const char* partitions_file = argv[5];
	out_file = argv[6];

	

	// Open the METIS input file
	std::ifstream in(filename);
	graph::metis_reader reader(in);


	/*mpi_process_group pg;
	std::ifstream in_partitions(partitions_file);
	boost::graph::metis_distribution dist(in_partitions, process_id(pg)); //this wont work.. how to create a process group??*/


	// Load the graph using the default distribution
	struct timeval start_time; 
	struct timeval end_time;

	
	gettimeofday(&start_time, NULL);
	Graph g(reader.begin(), reader.end(), reader.weight_begin(),
	        reader.num_vertices()/*, pg, dist*/);
	gettimeofday(&end_time, NULL); 

	float data_duration = (end_time.tv_sec - start_time.tv_sec) + (end_time.tv_usec - start_time.tv_usec) * 1E-6;

	if(g.processor() == 0)
	{
		std::cout<<filename<<":data:"<<data_duration<<std::endl;
	}

	gettimeofday(&start_time, NULL);
	chong_extract_high_betweenness(g, centrality_map(get(vertex_centrality,g))
	                               //.weight_map(get(edge_weight, g))
	                               //.buffer(q)
	                               .betweenness_extract_count(extract_count)
	                               .betweenness_batch_size(batch_count)
	                               .betweenness_max_stable_count(max_stable_count)
	                              );															
	gettimeofday(&end_time, NULL); 
	float algo_duration = (end_time.tv_sec - start_time.tv_sec) + (end_time.tv_usec - start_time.tv_usec) * 1E-6;
	if(g.processor() == 0)
	{
		std::cout<<filename<<":e_between:"<<algo_duration<<std::endl;
	}	
	
	
	/*
	// Output a Graphviz DOT file
  std::string outfile = filename;
	size_t i = outfile.rfind('.');
	if (i != std::string::npos)
		outfile.erase(outfile.begin() + i, outfile.end());
	outfile += "-dijkstra-2.dot";

  if (process_id(process_group(g)) == 0) {
	 int pcount = num_processes(process_group(g));
    std::cout << "Writing GraphViz output to " << outfile << "..." << pcount << "... ";
    std::cout.flush();
  }
  write_graphviz(outfile, g,
                 make_label_writer(get(vertex_centrality,g)),
                 make_label_writer(get(edge_weight, g)));
  if (process_id(process_group(g)) == 0)
    std::cout << "Done." << std::endl;
	*/
	return 0;
}
