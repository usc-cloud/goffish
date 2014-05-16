#include <stdio.h>

#include <boost/graph/use_mpi.hpp>

#include <boost/mpi.hpp>

#include <boost/graph/distributed/mpi_process_group.hpp>

#include <boost/graph/distributed/adjacency_list.hpp>

#include <boost/graph/metis.hpp>

#include <sys/time.h>
#include <boost/graph/graphviz.hpp>

#include "parallel_safe_leaf_compression.hpp"

#include "parallel_safe_partitioned_sssp.hpp"


using namespace boost;
using boost::graph::distributed::mpi_process_group;

#ifdef BOOST_NO_EXCEPTIONS
void
boost::throw_exception(std::exception const& ex)
{
	std::cout << ex.what() << std::endl;
	abort();
}
#endif

int main(int argc, char **argv)
{
	
	mpi::environment env(argc, argv);
	//mpi_process_group pg;

	typedef adjacency_list<vecS,
	        distributedS<mpi_process_group, vecS>,
	        undirectedS,
	        property<vertex_centrality_t,float, property<vertex_distance_t, float> >,
	        property<edge_weight_t, int> > Graph;
	
	typedef graph_traits<Graph>::vertices_size_type vertices_size_type;
	vertices_size_type extract_count = std::atoi(argv[1]);
	vertices_size_type max_stable_count = std::atoi(argv[2]);
	vertices_size_type batch_count = std::atoi(argv[3]);
	vertices_size_type delta = 1;
	
	const char* filename = argv[4];
	const char* partitions_file = argv[5];
	int do_compress = std::atoi(argv[6]);
	
	std::ifstream in(filename);
	graph::metis_reader reader(in);


	mpi_process_group pg;
	std::ifstream in_partitions(partitions_file);
	boost::graph::metis_distribution dist(in_partitions, process_id(pg));
	
	
	struct timeval start_time; 
	struct timeval end_time; 

	gettimeofday(&start_time, NULL);
	Graph g(reader.begin(), reader.end(), reader.weight_begin(), reader.num_vertices(), pg, dist);
	gettimeofday(&end_time, NULL);
	
	
	//std::cout<<"pid:"<<g.processor()<<" v:"<<num_vertices(g)<<" e:"<<num_edges(g)<<std::endl;
	
	//return 0;
	
	//end add edes
	float data_duration = (end_time.tv_sec - start_time.tv_sec) + (end_time.tv_usec - start_time.tv_usec) * 1E-6;
	
	if(g.processor() == 0)
	{
		std::cout<<filename<<":data:"<<data_duration<<std::endl;
	}
	

	typedef graph_traits<Graph>::edges_size_type edges_size_type;

	if(do_compress > 0)
	{
		edges_size_type edge_cnt = num_edges(g);
			
		edge_cnt = all_reduce(process_group(g), edge_cnt, boost::parallel::sum<edges_size_type>());
		
		if(g.processor() == 0)
		{
			std::cout<<"Edge Cnt before:"<<edge_cnt<<std::endl;
		}
		
		gettimeofday(&start_time, NULL);

		//printGraph(g);
					
		compress_graph_leaves(g);
		
		synchronize(g);
		
		edge_cnt = num_edges(g);
			
		edge_cnt = all_reduce(process_group(g), edge_cnt, boost::parallel::sum<edges_size_type>());
		
		if(g.processor() == 0)
		{
			std::cout<<"Edge Cnt after:"<<edge_cnt<<std::endl;
		}

	
		gettimeofday(&end_time, NULL);
		
		float leaf_duration = (end_time.tv_sec - start_time.tv_sec) + (end_time.tv_usec - start_time.tv_usec) * 1E-6;
		if(g.processor() == 0)
		{
			std::cout<<filename<<":leaf:"<<leaf_duration <<std::endl;
		}	
	}
	
	//return 0;
	
	gettimeofday(&start_time, NULL);
	
	/*partiotined_betweenness_centrality(g, 
				//boost::make_iterator_property_map(c.begin(), get(boost::vertex_index, g)),
				get(boost::vertex_centrality, g),
				get(boost::vertex_index, g));*/
				
	partiotined_chong_extract_high_centrality(g, 
				//boost::make_iterator_property_map(c.begin(), get(boost::vertex_index, g)),
				get(boost::vertex_centrality, g),
				get(boost::vertex_index, g),
				extract_count,
				batch_count,
				delta,
				max_stable_count
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
	/*BGL_FORALL_VERTICES_T(current_vertex, g, Graph) {
				// Default all distances to infinity
				//perSourceProps->put_distance(current_vertex, inf);
		
				// Default all vertex predecessors to the vertex itself
				// put(predecessor_map, current_vertex, current_vertex);
				std::cout<<process_id(process_group(g))<<":"<<pspsssp.get_global_id(current_vertex)<<":"<<perSourceProps->get_distance(current_vertex)<<std::endl;
	 }*/
	 return 0;
}
