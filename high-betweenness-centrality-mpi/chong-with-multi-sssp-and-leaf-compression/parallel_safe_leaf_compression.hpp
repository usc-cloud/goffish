#ifndef PARALLEL_SAFE_LEAF_COMPRESSION_HPP
#define PARALLEL_SAFE_LEAF_COMPRESSION_HPP

#include <boost/graph/distributed/distributed_graph_utility.hpp>

#include <boost/pending/relaxed_heap.hpp>
#include <boost/pending/indirect_cmp.hpp>

//#include <boost/random/linear_congruential.hpp>
//ALOK - Needed for randomize_vertex_order
#include <boost/random/mersenne_twister.hpp>
#include <boost/random/uniform_int_distribution.hpp>
#include <omp.h>

//NOTES
//1. typename std::iterator_traits<RAIter>::value_type for distance type for example

namespace boost
{
	
	using boost::graph::distributed::mpi_process_group;	

	template<typename Graph>
	void compress_graph_leaves(Graph &g)
	{
			typedef typename graph_traits<Graph>::vertex_descriptor VertexDescriptor;
			typedef typename graph_traits<Graph>::vertex_iterator vertex_iterator;
			typedef typename graph_traits<Graph>::degree_size_type degree_size_type;			
		
			mpi_process_group pg = process_group(g);

			int updates = 1;			
			while(updates > 0)
			{			
				updates = 0;
				BGL_FORALL_VERTICES_T(cv, g, Graph) {
					degree_size_type out_d = out_degree(cv, g);
					if(out_d == 1)
					{
						//remove_edge(*(out_edges(cv, g).first), g);
						clear_vertex(cv, g);
						updates=1;
					}
					else if(out_d > 1)
					{
						//see if all out edges are parallel edges. remove those too.
						degree_size_type parcnt = 0;
						VertexDescriptor targetV;
						VertexDescriptor prevTarget = target(*(out_edges(cv, g).first), g);
						BGL_FORALL_OUTEDGES_T(cv, ce, g, Graph) {
							targetV = target(ce, g);
							if(targetV == prevTarget)
							{
								parcnt++;
							}
							else
								break;
						}
						
						if(parcnt == out_d) //all parallel edges
						{
							clear_vertex(cv, g);
							updates = 1;
						}
					}
				}
				
				updates = all_reduce(pg, updates, boost::parallel::sum<int>());
			}
	}
	
	
	template<typename vertices_size_type, typename VertexDescriptor, typename Graph>
	vertices_size_type get_global_id(VertexDescriptor v, Graph& g, vertices_size_type /*dummy*/)
	{
			return g.distribution().global(owner(v), local(v));
	}
	
	template<typename Graph>
	void printGraph(Graph &g)
	{
		typedef typename graph_traits<Graph>::vertices_size_type vertices_size_type;
		BGL_FORALL_VERTICES_T(cv, g, Graph) {
			std::cout<<g.processor()<<"->"<<get_global_id(cv, g, num_vertices(g))<<":";
			BGL_FORALL_OUTEDGES_T(cv, ce, g, Graph) {
				std::cout<<get_global_id(target(ce, g), g, num_vertices(g))<<",";
			}
			std::cout<<std::endl;
		}
	}
}


#endif // PARALLEL_SAFE_PARTITIONED_SSSP_HPP
