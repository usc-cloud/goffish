// Copyright 2004 The Trustees of Indiana University.

// Distributed under the Boost Software License, Version 1.0.
// (See accompanying file LICENSE_1_0.txt or copy at
// http://www.boost.org/LICENSE_1_0.txt)

//  Authors: Alok Kumbhare

#ifndef BOOST_GRAPH_PARALLEL_CHONG_EXTRACT_HIGH_BETWEENNESS_HPP
#define BOOST_GRAPH_PARALLEL_CHONG_EXTRACT_HIGH_BETWEENNESS_HPP

#ifndef BOOST_GRAPH_USE_MPI
#error "Parallel BGL files should not be included unless <boost/graph/use_mpi.hpp> has been included"
#endif

// #define COMPUTE_PATH_COUNTS_INLINE

//#include <boost/graph/named_function_params.hpp>
#include "named_function_params.hpp"

#include <boost/graph/betweenness_centrality.hpp>
#include <boost/graph/overloading.hpp>
#include <boost/graph/distributed/concepts.hpp>
#include <boost/graph/graph_traits.hpp>
#include <boost/config.hpp>
#include <boost/assert.hpp>

// For additive_reducer
#include <boost/graph/distributed/distributed_graph_utility.hpp>
#include <boost/type_traits/is_convertible.hpp>
#include <boost/type_traits/is_same.hpp>
#include <boost/property_map/property_map.hpp>


#include <boost/property_map/parallel/distributed_property_map.hpp>

#include <boost/graph/distributed/detail/dijkstra_shortest_paths.hpp>
#include <boost/tuple/tuple.hpp>

// NGE - Needed for minstd_rand at L807, should pass vertex list
//       or generator instead
#include <boost/random/linear_congruential.hpp>

//ALOK - Needed for randomize_vertex_order
#include <boost/random/mersenne_twister.hpp>
#include <boost/random/uniform_int_distribution.hpp>

//Extending this. Reuse most of the code
#include <boost/graph/distributed/betweenness_centrality.hpp>

#include <algorithm>
#include <stack>
#include <vector>
#include <iostream>
#include <fstream>

extern char* out_file;

namespace boost
{

	namespace graph
	{
		namespace parallel
		{
			namespace detail
			{
//ALOK: A bug.. fix this. All processes should generate the random numbers in same order..
				boost::random::mt19937 gen;

				template<typename vertices_size_type>
				std::vector<vertices_size_type> randomize_vertex_order(vertices_size_type n)
				{
					boost::random::uniform_int_distribution<> rand_vertex(0,n-1);
					std::vector<vertices_size_type>	ordered_vertices;
					vertices_size_type i = 0;
					while(i < n) {
						vertices_size_type vidx = rand_vertex(gen);
						if(std::find(ordered_vertices.begin(), ordered_vertices.end(), vidx) == ordered_vertices.end()) {
							ordered_vertices.push_back(vidx);
							i++;
						}
					}
					return ordered_vertices;
				}

				
				template<typename vertices_size_type, typename centrality_type>
				void add_to_pair_list(std::vector<std::pair<vertices_size_type, centrality_type> >& pair_list, vertices_size_type max_size, vertices_size_type nv, centrality_type nc)
				{
					typedef typename std::vector<std::pair<vertices_size_type, centrality_type> > pair_list_type;

					//put at correct place
					typedef typename pair_list_type::iterator pair_iterator;
					pair_iterator it = pair_list.begin();

					for( ; it != pair_list.end(); ++it) {
						//vertices_size_type vid = it->first;
						centrality_type cent = it->second;
						if(nc > cent)
							break;
					}

					pair_list.insert(it, std::make_pair(nv,nc));

					if(pair_list.size() > max_size) {
						pair_list.pop_back(); //remove the smallest
					}
				}

//TODO: There is a lot of code copy from betweenness_impl function. Try to find a way to avoid that
				template<typename Graph, typename CentralityMap, typename EdgeCentralityMap,
				         typename IncomingMap, typename DistanceMap, typename DependencyMap,
				         typename PathCountMap, typename VertexIndexMap, typename ShortestPaths,
				         typename Buffer, typename vertices_size_type>
				void
				chong_extract_high_betweenness_impl(const Graph& g,
				                                    CentralityMap centrality,
				                                    EdgeCentralityMap edge_centrality_map,
				                                    IncomingMap incoming,
				                                    DistanceMap distance,
				                                    DependencyMap dependency,
				                                    PathCountMap path_count,
				                                    VertexIndexMap vertex_index,
				                                    ShortestPaths shortest_paths,
				                                    Buffer sources,
				                                    vertices_size_type extract_count,
				                                    vertices_size_type batch_count,
				                                    vertices_size_type max_stable_count
				                                   )
				{
					using boost::detail::graph::init_centrality_map;
					using boost::detail::graph::divide_centrality_by_two;
					using boost::graph::parallel::process_group;

					typedef typename graph_traits<Graph>::vertex_descriptor vertex_descriptor;

					typedef typename property_traits<DistanceMap>::value_type distance_type;
					typedef typename property_traits<DependencyMap>::value_type dependency_type;

					typedef typename boost::graph::parallel::process_group_type<Graph>::type
					process_group_type;
					process_group_type pg = process_group(g);
					
					unsigned long int seed;

					if(g.processor() ==  0)
						seed = time(NULL);
						
					broadcast(communicator(pg), seed, 0);
					gen.seed(seed);
					
					// Initialize centrality
					init_centrality_map(vertices(g), centrality);
					init_centrality_map(edges(g), edge_centrality_map);

					// Set the reduction operation on the dependency map to be addition
					dependency.set_reduce(boost::graph::distributed::additive_reducer<dependency_type>());
					distance.set_reduce(boost::graph::distributed::choose_min_reducer<distance_type>());

					// Don't allow remote procs to write incoming or path_count maps
					// updating them is handled inside the betweenness_centrality_queue
					incoming.set_consistency_model(0);
					path_count.set_consistency_model(0);




					if (!sources.empty()) {
						// DO SSSPs
						while (!sources.empty()) {
							do_brandes_sssp(g, centrality, edge_centrality_map, incoming, distance,
							                dependency, path_count,
							                vertex_index, shortest_paths, sources.top());
							sources.pop();
						}
					}
					else {   // Exact Betweenness Centrality

						vertices_size_type n = num_vertices(g);
						n = boost::parallel::all_reduce(pg, n, std::plus<vertices_size_type>());

						std::vector<vertices_size_type> random_ordered_vertices = randomize_vertex_order(n);
						
						//std::cout<<"Done randomize"<<std::endl;
						
						typedef typename boost::graph::parallel::process_group_type<Graph>::type
						process_group_type;
						typename process_group_type::process_id_type pid = process_id(pg);


						std::set<vertices_size_type> prevSet;
						std::set<vertices_size_type> currSet;

						vertices_size_type stable_count = 0;

						typedef typename mpl::if_c<(is_same<CentralityMap,
																				dummy_property_map>::value),
																				EdgeCentralityMap,
																				CentralityMap>::type a_centrality_map;

						typedef typename property_traits<a_centrality_map>::value_type centrality_type;
						
						typedef typename graph_traits<Graph>::vertex_iterator vertex_iterator;

						typedef typename std::pair<vertices_size_type, centrality_type> centrality_pair_type;
						
						std::vector<centrality_pair_type> local_max_topn;	
					
						int converged = 0;
						
						std::vector<centrality_pair_type> aggregated_top_n;
						std::vector<centrality_pair_type> collected_top;
						vertices_size_type vi = 0;
						for (; vi < n; ++vi) {
							vertices_size_type vid = random_ordered_vertices[vi];
							vertex_descriptor vertex_ = vertex(vid, g);

							do_brandes_sssp(g, centrality, edge_centrality_map, incoming, distance,
							                dependency, path_count,
							                vertex_index, shortest_paths, vertex_);

							//gather maxes pairs here..
							if(vi != 0 && vi%batch_count == 0) {
								//traverse centrality map (for vertices), we ignore edge_centrality for now
								// Find max centrality ALOK

								local_max_topn.clear();
								
								vertex_iterator v, v_end;
								for (boost::tie(v, v_end) = vertices(g); v != v_end; ++v) {
									centrality_type c_centality = get(centrality, *v);
									//c_vid is the global id of the vertex
									vertices_size_type c_vid = g.distribution().global(owner(*v), local(*v));

									add_to_pair_list(local_max_topn, extract_count, c_vid, c_centality);								
								}
								

								//Gather one at a time only from the highest to lowest ones
								int done = 0;
								aggregated_top_n.clear();
								vertices_size_type topi = 0;
								//for(int topi = 0; topi < local_max_topn.size(); topi++)
								while(done == 0)
								{
									
									centrality_pair_type to_send;
									if(topi < local_max_topn.size())
									{
										to_send = local_max_topn[topi];
									}
									else
									{
										to_send = std::make_pair(std::numeric_limits<vertices_size_type>::max(),-1);
									}
									
									collected_top.clear();
									if(pid == 0)
									{
										gather(communicator(pg), to_send, collected_top, 0);
										
										for(int i = 0; i < collected_top.size(); i++) {
											centrality_pair_type cpair = collected_top[i];
											add_to_pair_list(aggregated_top_n, extract_count, cpair.first, cpair.second);
										}
									}
									else
									{
										//gather(pg, local_max_topn[topi], 0);
										gather(communicator(pg), local_max_topn[topi], 0);
									}
									
									centrality_pair_type min;
									if(pid == 0)
									{							
										min = aggregated_top_n.back();
									}	
									broadcast(communicator(pg), min, 0);
									
									int incremented = 0;
									if(to_send.second >= min.second)
									{
										topi++;
										incremented = 1;
									}
									
									incremented = all_reduce(pg, incremented, boost::parallel::sum<int>());
									if(incremented == 0)
									{
										break; //break out of 
									}
								}

								if(pid == 0)
								{
									currSet.clear();
									for(int i = 0; i < aggregated_top_n.size(); i++) {
										centrality_pair_type cpair = aggregated_top_n[i];
										currSet.insert(cpair.first);
										local_put(centrality, vertex(cpair.first,g), cpair.second);
									}
								
									std::vector<vertices_size_type> v_intersection;
									set_intersection(currSet.begin(),currSet.end(),prevSet.begin(),prevSet.end(),
										std::back_inserter(v_intersection));
									
									if((currSet.size() - v_intersection.size()) <= 1) { //? TODO: get the delta count from user
										stable_count++;
									}
									else
									{
										stable_count = 0;	//Check if sotera does the same thing (oi.e. rest counter to 0)
									}
									
									if(stable_count >= max_stable_count) {
										//std::cout<<pid<<": Constant top set. Stoppping"<<std::endl;
										converged = 1;
									}
								}

								broadcast(communicator(pg), converged, 0);

								if(converged == 1)
								{
									break; //all done.. break the vertex loop
								}
								
								prevSet.clear();
								prevSet.insert(currSet.begin(),currSet.end());
								
								//Break after one batch for now to test perf.. 
								//break;
							}
						}
						
						if (pid == 0) {
								//std::cout<<pid<<":Writing out file"<<std::endl;
								//std::ofstream myfile;
								//myfile.open (out_file);
								std::cout<<"***final***"<<std::endl;
								typename std::set<vertices_size_type>::iterator itr = currSet.begin();
								for(; itr != currSet.end(); ++itr) {
									std::cout<<*itr<<":"<<get(centrality, vertex(*itr,g))<<std::endl;
								}
								//myfile.flush();
								//myfile.close();
								//std::cout<<pid<<":Writing done"<<std::endl;
								
								std::cout<<std::endl;
								std::cout<<g.processor()<<":num_sssp:"<<vi<<std::endl;
						}
					}

					typedef typename graph_traits<Graph>::directed_category directed_category;
					const bool is_undirected =
					  is_convertible<directed_category*, undirected_tag*>::value;
					if (is_undirected) {
						divide_centrality_by_two(vertices(g), centrality);
						divide_centrality_by_two(edges(g), edge_centrality_map);
					}
				}


			}
		}
	} // end namespace graph::parallel::detail

	template<typename Graph, typename CentralityMap, typename EdgeCentralityMap,
	         typename IncomingMap, typename DistanceMap, typename DependencyMap,
	         typename PathCountMap, typename VertexIndexMap, typename Buffer, typename VerticesSizeType>
	void
	chong_extract_high_betweenness(const Graph& g,
	                               CentralityMap centrality,
	                               EdgeCentralityMap edge_centrality_map,
	                               IncomingMap incoming,
	                               DistanceMap distance,
	                               DependencyMap dependency,
	                               PathCountMap path_count,
	                               VertexIndexMap vertex_index,
	                               Buffer sources,
	                               typename property_traits<DistanceMap>::value_type delta,
	                               VerticesSizeType extract_count,
	                               VerticesSizeType batch_count,
	                               VerticesSizeType max_stable_count
	                               BOOST_GRAPH_ENABLE_IF_MODELS_PARM(Graph,distributed_graph_tag))
	{
		typedef typename property_traits<DistanceMap>::value_type distance_type;
		typedef static_property_map<distance_type> WeightMap;

		graph::parallel::detail::brandes_shortest_paths<WeightMap>
		shortest_paths(delta);

		graph::parallel::detail::chong_extract_high_betweenness_impl(g, centrality,
		    edge_centrality_map,
		    incoming, distance,
		    dependency, path_count,
		    vertex_index,
		    shortest_paths,
		    sources,
		    extract_count,
		    batch_count,
		    max_stable_count);
	}

	template<typename Graph, typename CentralityMap, typename EdgeCentralityMap,
	         typename IncomingMap, typename DistanceMap, typename DependencyMap,
	         typename PathCountMap, typename VertexIndexMap, typename WeightMap,
	         typename Buffer, typename VerticesSizeType>
	void
	chong_extract_high_betweenness(const Graph& g,
	                               CentralityMap centrality,
	                               EdgeCentralityMap edge_centrality_map,
	                               IncomingMap incoming,
	                               DistanceMap distance,
	                               DependencyMap dependency,
	                               PathCountMap path_count,
	                               VertexIndexMap vertex_index,
	                               Buffer sources,
	                               typename property_traits<WeightMap>::value_type delta,
	                               WeightMap weight_map,
	                               VerticesSizeType extract_count,
	                               VerticesSizeType batch_count,
	                               VerticesSizeType max_stable_count
	                               BOOST_GRAPH_ENABLE_IF_MODELS_PARM(Graph,distributed_graph_tag))
	{
		graph::parallel::detail::brandes_shortest_paths<WeightMap> shortest_paths(weight_map, delta);

		graph::parallel::detail::chong_extract_high_betweenness_impl(g, centrality,
		    edge_centrality_map,
		    incoming, distance,
		    dependency, path_count,
		    vertex_index,
		    shortest_paths,
		    sources,
		    extract_count,
		    batch_count,
		    max_stable_count);
	}

	namespace graph
	{
		namespace parallel
		{
			namespace detail
			{
				template<typename Graph, typename CentralityMap, typename EdgeCentralityMap,
				         typename WeightMap, typename VertexIndexMap, typename Buffer, typename VerticesSizeType>
				void
				chong_extract_high_betweenness_dispatch2(const Graph& g,
				    CentralityMap centrality,
				    EdgeCentralityMap edge_centrality_map,
				    WeightMap weight_map,
				    VertexIndexMap vertex_index,
				    Buffer sources,
				    typename property_traits<WeightMap>::value_type delta,
				    VerticesSizeType extract_count,
				    VerticesSizeType batch_count,
				    VerticesSizeType max_stable_count)
				{
					typedef typename graph_traits<Graph>::degree_size_type degree_size_type;
					typedef typename graph_traits<Graph>::vertex_descriptor vertex_descriptor;
					typedef typename mpl::if_c<(is_same<CentralityMap,
					                            dummy_property_map>::value),
					                                               EdgeCentralityMap,
					                                               CentralityMap>::type a_centrality_map;
					typedef typename property_traits<a_centrality_map>::value_type
					centrality_type;

					typename graph_traits<Graph>::vertices_size_type V = num_vertices(g);

					std::vector<std::vector<vertex_descriptor> > incoming(V);
					std::vector<centrality_type> distance(V);
					std::vector<centrality_type> dependency(V);
					std::vector<degree_size_type> path_count(V);

					chong_extract_high_betweenness(
					  g, centrality, edge_centrality_map,
					  make_iterator_property_map(incoming.begin(), vertex_index),
					  make_iterator_property_map(distance.begin(), vertex_index),
					  make_iterator_property_map(dependency.begin(), vertex_index),
					  make_iterator_property_map(path_count.begin(), vertex_index),
					  vertex_index, unwrap_ref(sources), delta,
					  weight_map,
					  unwrap_ref(extract_count),
					  unwrap_ref(batch_count),
					  unwrap_ref(max_stable_count));
				}

// TODO: Should the type of the distance and dependency map depend on the
//       value type of the centrality map?
				template<typename Graph, typename CentralityMap, typename EdgeCentralityMap,
				         typename VertexIndexMap, typename Buffer, typename VerticesSizeType>
				void
				chong_extract_high_betweenness_dispatch2(const Graph& g,
				    CentralityMap centrality,
				    EdgeCentralityMap edge_centrality_map,
				    VertexIndexMap vertex_index,
				    Buffer sources,
				    typename graph_traits<Graph>::edges_size_type delta,
				    VerticesSizeType extract_count,
				    VerticesSizeType batch_count,
				    VerticesSizeType max_stable_count)
				{
					typedef typename graph_traits<Graph>::degree_size_type degree_size_type;
					typedef typename graph_traits<Graph>::edges_size_type edges_size_type;
					typedef typename graph_traits<Graph>::vertex_descriptor vertex_descriptor;

					typename graph_traits<Graph>::vertices_size_type V = num_vertices(g);

					std::vector<std::vector<vertex_descriptor> > incoming(V);
					std::vector<edges_size_type> distance(V);
					std::vector<edges_size_type> dependency(V);
					std::vector<degree_size_type> path_count(V);

					chong_extract_high_betweenness(
					  g, centrality, edge_centrality_map,
					  make_iterator_property_map(incoming.begin(), vertex_index),
					  make_iterator_property_map(distance.begin(), vertex_index),
					  make_iterator_property_map(dependency.begin(), vertex_index),
					  make_iterator_property_map(path_count.begin(), vertex_index),
					  vertex_index, unwrap_ref(sources), delta,
					  extract_count,
					  batch_count,
					  max_stable_count);
				}

				template<typename WeightMap>
				struct chong_extract_high_betweenness_dispatch1 {
					template<typename Graph, typename CentralityMap, typename EdgeCentralityMap,
					         typename VertexIndexMap, typename Buffer, typename VerticesSizeType>
					static void
					run(const Graph& g, CentralityMap centrality, EdgeCentralityMap edge_centrality_map,
					    VertexIndexMap vertex_index, Buffer sources,
					    typename property_traits<WeightMap>::value_type delta, WeightMap weight_map,
					    VerticesSizeType extract_count,
					    VerticesSizeType batch_count,
					    VerticesSizeType max_stable_count) {
						boost::graph::parallel::detail::chong_extract_high_betweenness_dispatch2(
						  g, centrality, edge_centrality_map, weight_map, vertex_index, sources, delta,
						  extract_count,
						  batch_count,
						  max_stable_count);
					}
				};

				template<>
				struct chong_extract_high_betweenness_dispatch1<boost::param_not_found> {
					template<typename Graph, typename CentralityMap, typename EdgeCentralityMap,
					         typename VertexIndexMap, typename Buffer, typename VerticesSizeType>
					static void
					run(const Graph& g, CentralityMap centrality, EdgeCentralityMap edge_centrality_map,
					    VertexIndexMap vertex_index, Buffer sources,
					    typename graph_traits<Graph>::edges_size_type delta,
					    boost::param_not_found,
					    VerticesSizeType extract_count,
					    VerticesSizeType batch_count,
					    VerticesSizeType max_stable_count) {
						boost::graph::parallel::detail::chong_extract_high_betweenness_dispatch2(
						  g, centrality, edge_centrality_map, vertex_index, sources, delta,
						  extract_count,
						  batch_count,
						  max_stable_count);
					}
				};

			}
		}
	} // end namespace graph::parallel::detail

	template<typename Graph, typename Param, typename Tag, typename Rest>
	void
	chong_extract_high_betweenness(const Graph& g,
	                               const bgl_named_params<Param,Tag,Rest>& params
	                               BOOST_GRAPH_ENABLE_IF_MODELS_PARM(Graph,distributed_graph_tag))
	{
		typedef bgl_named_params<Param,Tag,Rest> named_params;

		typedef queue<typename graph_traits<Graph>::vertex_descriptor> queue_t;
		queue_t q;

		typedef typename get_param_type<edge_weight_t, named_params>::type ew_param;
		typedef typename detail::choose_impl_result<mpl::true_, Graph, ew_param, edge_weight_t>::type ew;
		graph::parallel::detail::chong_extract_high_betweenness_dispatch1<ew>::run(
		  g,
		  choose_param(get_param(params, vertex_centrality),
		               dummy_property_map()),
		  choose_param(get_param(params, edge_centrality),
		               dummy_property_map()),
		  choose_const_pmap(get_param(params, vertex_index), g, vertex_index),
		  choose_param(get_param(params, buffer_param_t()), boost::ref(q)),
		  choose_param(get_param(params, lookahead_t()), 0),
		  choose_const_pmap(get_param(params, edge_weight), g, edge_weight),
		  choose_param(get_param(params, betweenness_extract_count_t()), 1),
		  choose_param(get_param(params, betweenness_batch_size_t()), 1),
		  choose_param(get_param(params, betweenness_max_stable_count_t()), 1));

	}

	template<typename Graph, typename CentralityMap, typename VerticesSizeType>
	void
	chong_extract_high_betweenness(const Graph& g, CentralityMap centrality,
	                               VerticesSizeType extract_count,
	                               VerticesSizeType batch_count,
	                               VerticesSizeType max_stable_count
	                               BOOST_GRAPH_ENABLE_IF_MODELS_PARM(Graph,distributed_graph_tag))
	{
		typedef queue<typename graph_traits<Graph>::vertex_descriptor> queue_t;
		queue_t q;

		boost::graph::parallel::detail::chong_extract_high_betweenness_dispatch2(
		  g, centrality, dummy_property_map(), get(vertex_index, g), boost::ref(q), 0,
		  extract_count,
		  batch_count,
		  max_stable_count);
	}

	template<typename Graph, typename CentralityMap, typename EdgeCentralityMap, typename VerticesSizeType>
	void
	chong_extract_high_betweenness(const Graph& g, CentralityMap centrality,
	                               EdgeCentralityMap edge_centrality_map,
	                               VerticesSizeType extract_count,
	                               VerticesSizeType batch_count,
	                               VerticesSizeType max_stable_count
	                               BOOST_GRAPH_ENABLE_IF_MODELS_PARM(Graph,distributed_graph_tag))
	{
		typedef queue<int> queue_t;
		queue_t q;

		boost::graph::parallel::detail::chong_extract_high_betweenness_dispatch2(
		  g, centrality, edge_centrality_map, get(vertex_index, g), boost::ref(q), 0,
		  extract_count,
		  batch_count,
		  max_stable_count);
	}

} // end namespace boost

#endif // BOOST_GRAPH_PARALLEL_CHONG_EXTRACTING_HIGH_BETWEENNESS_HPP
