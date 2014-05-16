#ifndef PARALLEL_SAFE_PARTITIONED_SSSP_HPP
#define PARALLEL_SAFE_PARTITIONED_SSSP_HPP

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
extern char* out_file;
namespace boost
{
	
	using boost::graph::distributed::mpi_process_group;
	
	template <typename T>
	struct append_reducer { //for the pred map. mainly to get the default value. we dont really need the reduce because we only use put_local
		BOOST_STATIC_CONSTANT(bool, non_default_resolver = true);

		template<typename K>
		T operator()(const K&) const { return T(); }

		template<typename K>
		T operator()(const K&, const T& x, const T& y) const
		{
			T z(x.begin(), x.end());
			for (typename T::const_iterator iter = y.begin(); iter != y.end(); ++iter)
				if (std::find(z.begin(), z.end(), *iter) == z.end())
					z.push_back(*iter);

			return z;
		}
	};
	
	enum message_tags
	{
		distance_message,
		successor_message,
		betwen_message,
		path_count_message
	};
	
	/*    first         second.first   second.second.first    second.second.second
	 * (distance_to_v, (u,            (distance_to_u,         sssp_source))) 
	 * 
	 */
	template<typename distance_type, typename vertex_type>
	class parallel_safe_partitioned_sssp_msg_value
  {    
  public:
    typedef std::pair<distance_type, std::pair<vertex_type, std::pair<distance_type, std::pair<distance_type , vertex_type> > > > msg_type;
		//typedef std::tuple<distance_type /*distance to v*/, distance_type /*distance to u*/, distance_type /*path count to u*/, vertex_type /*vertex u*/, vertex_type /*sssp source vertex*/> msg_type;

    static msg_type create(distance_type dist_to_v, distance_type dist_to_u, distance_type path_count_to_u, vertex_type u, vertex_type sssp_source)
    { return std::make_pair(dist_to_v, std::make_pair(u, std::make_pair(dist_to_u, std::make_pair(path_count_to_u, sssp_source)))); }
	};
	
	
	template<typename vertex_type>
	class parallel_safe_partioned_successor_msg_value
	{
	public:
		typedef std::pair<vertex_type, std::pair<vertex_type, vertex_type> > msg_type;
		
		static msg_type create(vertex_type v /*remote vertex*/, vertex_type succ, vertex_type sssp_source)
		{
			return std::make_pair(v, std::make_pair(succ, sssp_source));
		}
	};


	template<typename vertex_type, typename path_count_type>
	class parallel_safe_partioned_path_count_msg_value
	{
	public:
		typedef std::pair<vertex_type, std::pair<vertex_type, std::pair<path_count_type, vertex_type> > > msg_type;
		
		static msg_type create(vertex_type v /*remote vertex*/, vertex_type self, path_count_type path_cnt, vertex_type ssspsource)
		{			
			return std::make_pair(v, std::make_pair(self, std::make_pair(path_cnt, ssspsource)));
		}
	};
	
	template<typename vertex_type, typename sigma_type, typename delta_type>
	class parallel_safe_partioned_between_msg_value
	{
		//int sigmaw = perSourceProps->get_path_count(w);
		//float delw = perSourceProps->get_delta(w);
	public:
		typedef std::pair<vertex_type, std::pair<sigma_type, std::pair<delta_type, std::pair<vertex_type, vertex_type> > > > msg_type;
		
		static msg_type create(vertex_type v /*remote vertex*/, vertex_type self, sigma_type mysigma, delta_type mydelta, vertex_type sssp_source)
		{
			return std::make_pair(v, std::make_pair(mysigma, std::make_pair(mydelta, std::make_pair(self, sssp_source))));
		}
	};
	
	struct sssp_property_maps_for_one_source_base
	{
		
	};
	
	template<typename T>
  struct my_additive_reducer_default_infinite {
      BOOST_STATIC_CONSTANT(bool, non_default_resolver = true);

      template<typename K>
      T operator()(const K&) const { return (std::numeric_limits<T>::max)(); }

      template<typename K>
      T operator()(const K&, const T& local, const T& remote) const { return local + remote; }
  };

	
	template<typename distance_map, typename predMap, typename path_countMap, typename succMap, typename DeltaMap>
	struct sssp_property_maps_for_one_source : public sssp_property_maps_for_one_source_base
	{
	private:
			//global id to distance map
			//std::map<int, int>* d;
			distance_map d;
			predMap pred;
			succMap succ;
			path_countMap path;
			DeltaMap del;
			path_countMap pred_count;
			
			std::vector<int> remote_vertices_seen_global_id;
			
			typedef typename property_traits<predMap>::value_type PredListType;	
			typedef typename property_traits<path_countMap>::value_type PathCountType;
			typedef typename property_traits<DeltaMap>::value_type DeltaType;
			typedef typename property_traits<distance_map>::value_type DistanceType;
			
			
	public:
			~sssp_property_maps_for_one_source()
			{
				remote_vertices_seen_global_id.clear();
				d.clear();
				pred.clear();
				succ.clear();
				path.clear();
				del.clear();
				//NOT SURE WHAT HAPPENS TO ALL THE MAPS?
			}
			
			sssp_property_maps_for_one_source(distance_map dm, predMap pm, path_countMap pcm, succMap sc, DeltaMap delm, path_countMap predc)
				: d(dm), pred(pm), path(pcm), succ(sc), del(delm), pred_count(predc) {
					pred.set_reduce(append_reducer<PredListType>());			
					path.set_reduce(boost::graph::distributed::additive_reducer<PathCountType>());
					del.set_reduce(boost::graph::distributed::additive_reducer<DeltaType>());
					pred_count.set_reduce(boost::graph::distributed::additive_reducer<PathCountType>());
					d.set_reduce(my_additive_reducer_default_infinite<DistanceType>());
			}
			
			void add_seen_remote_vertex(int global_id)
			{
				remote_vertices_seen_global_id.push_back(global_id);
			}
			
			bool is_remote_vertex_seen(int global_id)
			{
				std::vector<int>::iterator it = std::find (remote_vertices_seen_global_id.begin(), remote_vertices_seen_global_id.end(), global_id);
				if(it == remote_vertices_seen_global_id.end())
				{
						return false;
				}
				else
				{
					return true;
				}
			}
			
			template<typename VertexDescriptor,typename DType>
			void put_distance(VertexDescriptor v, DType distance)
			{
				local_put(d, v, distance);				
			}
			
			template<typename VertexDescriptor,typename DType>
			void cache_distance(VertexDescriptor v, DType distance)
			{
				cache(d, v, distance);
			}
			
			template<typename VertexDescriptor>
			DistanceType get_distance(VertexDescriptor v)
			{					
					/*if(d->count(gid) > 0)
						return (*d)[gid];
					else
						return (std::numeric_limits<int>::max)();*/
					return get(d, v);
			}
								
			distance_map get_distance_map()
			{
				return d;
			}
			
			template<typename VertexDescriptor>
			bool append_pred_if_not_exists(VertexDescriptor v, VertexDescriptor p)
			{
					//v should strictly be local vertex.
					
					PredListType in = get(pred, v);
					//Will have to optimize this too instead of linear search
					if (std::find(in.begin(), in.end(), p) == in.end()) 
					{
					 in.push_back(p);
					 local_put(pred, v, in);
					 
					 //ALOK
					 PathCountType pred_c = get(pred_count, v);
					 local_put(pred_count, v, pred_c+1); //incremtn the pred count for v
					 
					 
					 return true;
					}
				 //return false;
			}
			
			template<typename VertexDescriptor>
			bool append_succ_if_not_exists(VertexDescriptor v, VertexDescriptor s)
			{
					//v should strictly be local vertex.					
					PredListType in = get(succ, v);
					if (std::find(in.begin(), in.end(), s) == in.end()) {
					 in.push_back(s);
					 local_put(succ, v, in);
					 
					 return true;
					}
				 return false;
			}
			
			template<typename VertexDescriptor>
			void clear_pred_list_and_add_one(VertexDescriptor v, VertexDescriptor singleP)
			{
					PredListType in = get(pred, v);
					in.clear();		 //assumes a vector.. ok for now. maybe not for a generic one
					in.push_back(singleP);
					local_put(pred, v, in);

					//ALOK
					local_put(pred_count, v, 1); //set the pred. count to 1
			}
			
			template<typename VertexDescriptor>
			PredListType get_predecessors(VertexDescriptor v)
			{
				PredListType preds = get(pred, v);
				return preds;
			}
			
			template<typename VertexDescriptor>
			PredListType get_successors(VertexDescriptor v)
			{
				PredListType succs = get(succ, v);
				return succs;
			}
			
			template<typename VertexDescriptor>
			bool remove_successor(VertexDescriptor v, VertexDescriptor s)
			{
					PredListType in = get(succ, v);
					typename PredListType::iterator fitr = std::find(in.begin(), in.end(), s);
					if(fitr != in.end())
					{
					 in.erase(fitr);
					 local_put(succ, v, in);
					 return true;
					}
					std::cout<<"SHOULD NEVER HAPPEN";
					abort();
				 return false;
			}
			
			template<typename VertexDescriptor>
			void decrement_pred_count(VertexDescriptor iv)
			{
				PathCountType piv = get(pred_count, iv);
				piv--;
				local_put(pred_count, iv, piv);
			}

			template<typename VertexDescriptor>
			PathCountType get_pred_count(VertexDescriptor iv)
			{
				return get(pred_count, iv);
			}
						
			template<typename VertexDescriptor, typename PathCType>
			void increment_path_count(VertexDescriptor v, PathCType pcu)
			{										
					PathCType pcv = get(path, v);
					pcv += pcu;
					local_put(path, v, pcv);
			}
			
			template<typename VertexDescriptor, typename PathCType>
			void set_path_count(VertexDescriptor v, PathCType pcu)
			{		
					PathCType pcv = get(path, v);
					pcv = pcu;
					local_put(path, v, pcv);
			}
			
			template<typename VertexDescriptor>
			PathCountType get_path_count(VertexDescriptor v)
			{
				PathCountType pcv = get(path, v);
				return pcv; //v should be local
			}
			
			template<typename VertexDescriptor>
			void set_delta(VertexDescriptor v, float delta)
			{
				local_put(del, v, delta);
			}
			
			template<typename VertexDescriptor>
			float get_delta(VertexDescriptor v)
			{				
				float dv = get(del, v);
				return dv; //v should be local
			}
			
			/*template<typename VertexDescriptor>
			void pushToLocalVStack(VertexDescriptor v)
			{
				localVStack.push(v);
			}
			
			VertexStack getLocalVStack()
			{
				return localVStack;
			}*/
	};

	template<typename Graph, typename CentralityMapType>
	class parallel_safe_partitioned_sssp
	{
		
	private:
		Graph& g;
		mpi_process_group pg;
		int pid;
		
		typedef typename graph_traits<Graph>::vertices_size_type vertices_size_type;			
		
		std::map<vertices_size_type, sssp_property_maps_for_one_source_base*> sssp_source_global_id_to_properties;
		CentralityMapType centrality_map;
	
		sssp_property_maps_for_one_source_base* get_per_source_properties(int global_vertex_id)
		{
			return sssp_source_global_id_to_properties.find(global_vertex_id)->second;
		}
		
		std::map<vertices_size_type, sssp_property_maps_for_one_source_base*> get_allsource_sssp_prop_map()
		{
			return sssp_source_global_id_to_properties;
		}
		
					 
	public:
		void clear_allsource_sssp_prop_map()
		{
			typename std::map<vertices_size_type, sssp_property_maps_for_one_source_base*>::iterator itr = sssp_source_global_id_to_properties.begin();
			for(;itr != sssp_source_global_id_to_properties.end(); ++itr)
			{
				delete (*itr).second;
			}
			sssp_source_global_id_to_properties.clear();
		}
	
		parallel_safe_partitioned_sssp(Graph& gp, CentralityMapType cm)
		:g(gp), pg(process_group(gp)), pid(process_id(pg)), centrality_map(cm)
		{
			//typedef graph_traits <Graph>::vertex_descriptor vertex_descriptor;
			typedef typename property_traits<CentralityMapType>::value_type CenType;
			centrality_map.set_reduce(boost::graph::distributed::additive_reducer<CenType>());
		}
		
		template<typename VertexDescriptor>
		vertices_size_type get_global_id(VertexDescriptor v)
		{
				return g.distribution().global(owner(v), local(v));
				//return get(global_index, v);
		}
		
		~parallel_safe_partitioned_sssp(){}
		
		bool sssp_property_exists_for_sssp_source(vertices_size_type global_verted_id)
		{
			return sssp_source_global_id_to_properties.count(global_verted_id);
		}
		
		void put_per_source_properties(vertices_size_type global_vertex_id, sssp_property_maps_for_one_source_base* props)
		{
			sssp_source_global_id_to_properties[global_vertex_id] = props;
		}
		
		template<typename VertexDescriptor, typename DistanceType, typename sssp_props>
		bool relax(VertexDescriptor u, VertexDescriptor v, DistanceType d, VertexDescriptor sssp_source, sssp_props props)
		{
			//std::cout<<owner(u)<<":val at v:"<<get_global_id(u)<<" and:"<<get_global_id(v)<<" v's owner:"<<owner(v)<<" wssssps:"<<get_global_id(sssp_source)<<"ad: "<<d<<" sd:"<< props->get_distance(v)<<std::endl;
			
			if(owner(v) == process_id(process_group(g))) //this should always be true.. assert this instead
			{
				if(d <= props->get_distance(v))
				{
					//std::cout<<"#"<<owner(u)<<":val at v:"<<get_global_id(u)<<" and:"<<get_global_id(v)<<" v's owner:"<<owner(v)<<" wssssps:"<<get_global_id(sssp_source)<<"ad: "<<d<<" sd:"<< props->get_distance(v)<<std::endl;
					
					if(d < props->get_distance(v))
					{
						props->put_distance(v, d);
						//ALOK
						props->clear_pred_list_and_add_one(v, u);
						return true;
					}
					else //equal. No need to relax. but increment path count
					{
						//ALOK
						bool appended = props->append_pred_if_not_exists(v, u);
						return false;
					}
				}
				return false;
			}
			
			
			return false;
		}
		
		

		
		template<typename DistanceMapType, typename PredMapType, typename PathCountMapType, typename DeltaMap, typename IndexMap>
		sssp_property_maps_for_one_source<DistanceMapType, PredMapType, PathCountMapType, PredMapType, DeltaMap>* 
		get_sssp_prop_map_for_source(vertices_size_type sssp_src_global_id, IndexMap index_map)
		{
			
			typedef typename property_traits<DistanceMapType>::value_type DistanceType;
			typedef typename property_traits<PredMapType>::value_type PredValueType;
			typedef typename property_traits<PathCountMapType>::value_type PathCountType;		
			
			sssp_property_maps_for_one_source<DistanceMapType, PredMapType, PathCountMapType, PredMapType, DeltaMap>* perSourceProps;
			if(sssp_property_exists_for_sssp_source(sssp_src_global_id))
			{
				//perSourceProps = (sssp_property_maps_for_one_source*) get_per_source_properties(sssp_src_global_id);
				perSourceProps = (sssp_property_maps_for_one_source<DistanceMapType, PredMapType, PathCountMapType, PredMapType, DeltaMap>*) get_per_source_properties(sssp_src_global_id);
			}
			else
			{
				std::vector<DistanceType>* d = new std::vector<DistanceType>(num_vertices(g));
				std::vector<PredValueType >* preds = new std::vector<PredValueType >(num_vertices(g));
				std::vector<PredValueType >* succs = new std::vector<PredValueType >(num_vertices(g));
				std::vector<PathCountType>* path = new std::vector<PathCountType>(num_vertices(g));
				std::vector<PathCountType>* pred_c = new std::vector<PathCountType>(num_vertices(g));
				std::vector<float>* dels = new std::vector<float>(num_vertices(g));
				//VStack* vstack = new VStack();

				 perSourceProps = new sssp_property_maps_for_one_source<DistanceMapType, PredMapType, PathCountMapType, PredMapType, DeltaMap>(
					boost::make_iterator_property_map(d->begin(), index_map),
					boost::make_iterator_property_map(preds->begin(), index_map),
					boost::make_iterator_property_map(path->begin(), index_map),
					boost::make_iterator_property_map(succs->begin(), index_map),
					boost::make_iterator_property_map(dels->begin(), index_map),
					boost::make_iterator_property_map(pred_c->begin(), index_map)
					/*, boost::make_iterator_property_map(p->begin(), index_map)*/
				);
			
				// Initialize vertices
				//NOT REQUIRED?
				BGL_FORALL_VERTICES_T(current_vertex, g, Graph) {
						// Default all distances to infinity
						DistanceType inf = (std::numeric_limits<DistanceType>::max)();
						perSourceProps->put_distance(current_vertex, inf);
				
						// Default all vertex predecessors to the vertex itself
						// put(predecessor_map, current_vertex, current_vertex);
				}
				
				put_per_source_properties(sssp_src_global_id, perSourceProps);
			}
			return perSourceProps;
		}
		
		/*int sssp_updates = parallel_safe_local_sssp(
						main_source,					
						vertex_queue,
						index_map,
						perSourceProps,
						get(boost::edge_weight, g)
					);*/
					
		template<typename VertexDescriptor, typename IndexMap, typename WeightMap, typename VertexQueue, typename PerSourceProps, typename DistanceCompare>
		int parallel_safe_local_sssp(VertexDescriptor sssp_source, 
			VertexQueue& vertex_queue,
			IndexMap index_map,
			PerSourceProps perSourceProps,
			DistanceCompare distance_compare,
			WeightMap weight_map, int remote = -1)
		{
			//sssp_property_maps_for_one_source<DistanceMap, PredMap> perSourcePropertyMaps(perSourceDisMap, perSourcePredMap);
			
			typedef typename DistanceCompare::first_argument_type DistanceType;
			
			int local_updates = 0;
			
			DistanceType inf = (std::numeric_limits<DistanceType>::max)();
			
			DistanceType distance_zero = DistanceType();
			
			int pid = process_id(process_group(g));
			
			while (!vertex_queue.empty()) {
				
				VertexDescriptor u = vertex_queue.top();
				
				vertex_queue.pop();

				//std::cout<<"looping"<<std::endl;
					
				// Check if any other vertices can be reached
				DistanceType min_vertex_distance = perSourceProps->get_distance(u); //get(perSourceProps->getDistanceMap(), u);
				
				if (!distance_compare(min_vertex_distance, inf)) {
					// This is the minimum vertex, so all other vertices are unreachable
					//return;
					break;
				}
		
				// Examine neighbors of min_vertex
				BGL_FORALL_OUTEDGES_T(u, current_edge, g, Graph) {

						
						//std::cout<<"EDGE START********"<<std::endl;
						//visitor.examine_edge(current_edge, graph);
						VertexDescriptor neighbor_vertex = target(current_edge, g);

						if(owner(neighbor_vertex) == process_id(pg))
						{
							if (distance_compare(get(weight_map, current_edge), distance_zero)) {
								//boost::throw_exception(negative_edge());
								std::cerr<<"Error OCC";							
								abort();
							}
				
							//std::cout<<"getting distance********:"<<get_global_id(neighbor_vertex)<<" pid:"<<process_id(pg)<<" owner:"<<owner(neighbor_vertex)<<std::endl;
							
							// Extract the neighboring vertex and get its distance
							DistanceType neighbor_vertex_distance =  perSourceProps->get_distance(neighbor_vertex);//get(perSourceProps->getDistanceMap(), neighbor_vertex);
							
							//std::cout<<"got distance********:"<<get_global_id(neighbor_vertex)<<std::endl;
							
							bool is_neighbor_undiscovered = !distance_compare(neighbor_vertex_distance, inf);

							DistanceType distance_to_u = perSourceProps->get_distance(u);
							DistanceType distance_to_neighbor_vertex =  distance_to_u + get(weight_map, current_edge);
							
							// Attempt to relax the edge
							//std::cout<<"relaxing"<<std::endl;
							
//							if(get_global_id(neighbor_vertex) == 5128 || get_global_id(neighbor_vertex) == 5041
//							 || get_global_id(u) == 5041 ||get_global_id(u) == 5128)
//							{
//								std::cout<<"beforelocally:v:"<<get_global_id(neighbor_vertex)<<":u:"<<get_global_id(u)<<":pc to u:"<<perSourceProps->get_path_count(u)
//								<<":pc to v:"<<perSourceProps->get_path_count(neighbor_vertex)<<std::endl;
//							}
							
							bool was_edge_relaxed = relax(u, neighbor_vertex, distance_to_neighbor_vertex, sssp_source, perSourceProps);
							
							//std::cout<<"relaxed"<<std::endl;
							
							
//						if(get_global_id(neighbor_vertex) == 5128 || get_global_id(neighbor_vertex) == 5041
//							 || get_global_id(u) == 5041 ||get_global_id(u) == 5128)
//							{
//								std::cout<<"locally:v:"<<get_global_id(neighbor_vertex)<<":u:"<<get_global_id(u)<<":pc to u:"<<perSourceProps->get_path_count(u)
//								<<":pc to v:"<<perSourceProps->get_path_count(neighbor_vertex)<<std::endl;
//							}
							/*if(remote != -1)
							{
								std::cout<<"before queue update:"<<remote<<std::endl;								
							}*/
							if (was_edge_relaxed) {
								++local_updates;
								
								//visitor.edge_relaxed(current_edge, graph);
								if (is_neighbor_undiscovered) {
									//visitor.discover_vertex(neighbor_vertex, graph);
									//std::cout<<"pushnig"<<std::endl;
									vertex_queue.push(neighbor_vertex);
									//std::cout<<"pushed"<<std::endl;
								} else {
									//std::cout<<"updating"<<std::endl;
									vertex_queue.update(neighbor_vertex);
									//std::cout<<"updated"<<std::endl;
								}
							}
							/*if(remote != -1)
							{
								std::cout<<"after queue:"<<remote<<std::endl;
								return 0;
							}*/
							//std::cout<<"EDGE END********"<<std::endl;
							
						}
						else //remote vertex
						{
							
							DistanceType distance_to_u = perSourceProps->get_distance(u);
							DistanceType distance_to_neighbor_vertex =  distance_to_u + get(weight_map, current_edge);
													
							//get the cache value for remote vertex
							/*bool isRemoteSeen = perSourceProps->is_remote_vertex_seen(get_global_id(neighbor_vertex));
							
							DistanceType oldD = std::numeric_limits<DistanceType>::max();
							if(isRemoteSeen)
							{
								oldD = perSourceProps->get_distance(neighbor_vertex);
							}
							
//							if(get_global_id(neighbor_vertex) == 5128 || get_global_id(neighbor_vertex) == 5041
//							 || get_global_id(u) == 5041 ||get_global_id(u) == 5128)
//							{
//								std::cout<<"tsending:"<<get_global_id(neighbor_vertex)<<":u:"<<get_global_id(u)<<":pc to u:"<<perSourceProps->get_path_count(u)<<std::endl;
//							}
							
							if(!isRemoteSeen || distance_to_neighbor_vertex <= oldD)*/
							//CAN BE OPTIMIZED BY CACHE>>> FOR NOW IGNORE
							DistanceType oldD = perSourceProps->get_distance(neighbor_vertex);
							//std::cout<<"from:"<<get_global_id(u)<<" for:"<<get_global_id(neighbor_vertex)<<" oldd:"<<oldD<<" newd:"<<distance_to_neighbor_vertex<<std::endl;
							if(distance_to_neighbor_vertex <= oldD)
							{
								typename parallel_safe_partitioned_sssp_msg_value<DistanceType, VertexDescriptor>::msg_type msg = 
									parallel_safe_partitioned_sssp_msg_value<DistanceType, VertexDescriptor>::create(distance_to_neighbor_vertex, 
									distance_to_u, 
										perSourceProps->get_path_count(u), u, sssp_source);
								
								//cache tentaiive distance to remote
								//perSourceProps->put_distance(neighbor_vertex, distance_to_neighbor_vertex);
								perSourceProps->cache_distance(neighbor_vertex, distance_to_neighbor_vertex);
								
								//std::cout<<"putting:"<<distance_to_neighbor_vertex<<" for:"<<get_global_id(neighbor_vertex)<<" reading:"<<perSourceProps->get_distance(neighbor_vertex)<<std::endl;
								//abort();
								
								int to_send_process = owner(neighbor_vertex);
								//std::cout<<"sending from v:"<<get_global_id(u)<<" to:"<<get_global_id(v)<<" dist:"<<d<<" wssssps:"<<get_global_id(sssp_source)<<std::endl;
								send(pg, to_send_process, distance_message, std::make_pair(neighbor_vertex, msg));
								
								//perSourceProps->add_seen_remote_vertex(get_global_id(neighbor_vertex));
							}
						}
				} // end out edge iteration
			} // end while queue not empty

			return local_updates;
		}
		
		template<typename IndexMap>
		int calculate_all_local_betweenness(IndexMap index_map)
		{
			typedef typename graph_traits<Graph>::vertices_size_type vertices_size_type;
			 std::map<vertices_size_type, sssp_property_maps_for_one_source_base*> all_source_props = get_allsource_sssp_prop_map();
			 typename std::map<vertices_size_type, sssp_property_maps_for_one_source_base*>::iterator itr = all_source_props.begin();
			 for(;itr != all_source_props.end(); ++itr)
			 {
				 //std::cout<<"Calculating for source:"<<(*itr).first<<std::endl;
				 calculate_local_betweenness(vertex((*itr).first, g), index_map);
			 }
		}
		
		template<typename VertexDescriptor, typename IndexMap>
		int calculate_local_betweenness(VertexDescriptor v, IndexMap index_map)
		{
			int updates = 0;
			int sssp_src_global_id = get_global_id(v);
			
			typedef iterator_property_map<std::vector<int>::iterator, IndexMap> DistanceMapType;
			typedef typename property_traits<DistanceMapType>::value_type DistanceType;
			typedef typename parallel_safe_partitioned_sssp_msg_value<DistanceType, VertexDescriptor>::msg_type internal_msg_type;
			
			
			
			typedef typename graph_traits<Graph>::degree_size_type degree_size_type;
			
			typedef typename std::vector<std::vector<VertexDescriptor> >::iterator VertexListIterator;
			typedef iterator_property_map<VertexListIterator, IndexMap> PredMapType;
			
			
			typedef typename std::vector<degree_size_type>::iterator DegreeIterator;
			typedef iterator_property_map<DegreeIterator, IndexMap> PathCountMapType;
			
			typedef typename std::vector<float>::iterator DeltaIterator;
			typedef iterator_property_map<DeltaIterator, IndexMap> DeltaMapType;
			
			sssp_property_maps_for_one_source<DistanceMapType, PredMapType, PathCountMapType, PredMapType, DeltaMapType>* perSourceProps;
			perSourceProps = get_sssp_prop_map_for_source<DistanceMapType, PredMapType, PathCountMapType, DeltaMapType, IndexMap>(sssp_src_global_id, index_map);
			
			
			//get vertices with no successors into a queue
			std::queue<VertexDescriptor> succ_queue;
			
			//initialize the queue where there are no successors
			BGL_FORALL_VERTICES_T(current_vertex, g, Graph) {

				//get successors
				std::vector<VertexDescriptor> succ = perSourceProps->get_successors(current_vertex);
				std::vector<VertexDescriptor> preds = perSourceProps->get_predecessors(current_vertex);
		
				if(succ.size() == 0 && preds.size() != 0)
				{
					//std::cout<<process_id(pg)<<":Adding successor:"<<get_global_id(current_vertex)<<std::endl;
					succ_queue.push(current_vertex);
				}
			}
			
			process_betweenness_reverse_pred(succ_queue, v, perSourceProps);
		}
		
		/*template<typename IndexMap>
		int update_all_pcs(IndexMap index_map)
		{
		 * typedef typename graph_traits<Graph>::vertices_size_type vertices_size_type;	
			 std::map<vertices_size_type, sssp_property_maps_for_one_source_base*> all_source_props = get_allsource_sssp_prop_map();
			 typename std::map<vertices_size_type, sssp_property_maps_for_one_source_base*>::iterator itr = all_source_props.begin();
			 for(;itr != all_source_props.end(); ++itr)
			 {
				 update_pcs(vertex((*itr).first, g), index_map);
			 }
		}*/
		
		template<typename IndexMap>
		int gather_all_local_successors(IndexMap index_map)
		{
				typedef typename graph_traits<Graph>::vertices_size_type vertices_size_type;
			 std::map<vertices_size_type, sssp_property_maps_for_one_source_base*> all_source_props = get_allsource_sssp_prop_map();
			 typename std::map<vertices_size_type, sssp_property_maps_for_one_source_base*>::iterator itr = all_source_props.begin();
			 for(;itr != all_source_props.end(); ++itr)
			 {
				 //std::cout<<g.processor()<<":gatheringfor:"<<(*itr).first<<std::endl;
				 gather_successors(vertex((*itr).first, g), index_map); //AAAAAAAAAAAAAHHHHHH
			 }
		}
		
		template<typename VertexDescriptor, typename IndexMap>
		int gather_successors(VertexDescriptor v, IndexMap index_map)
		{
			int updates = 0;
			int sssp_src_global_id = get_global_id(v); //AAAAAAAAAAAAAHHHHHH DO NOT MATCH>> :(
			//std::cout<<g.processor()<<":sssp_src_id:"<<sssp_src_global_id<<std::endl;
			
			typedef iterator_property_map<std::vector<int>::iterator, IndexMap> DistanceMapType;
			typedef typename property_traits<DistanceMapType>::value_type DistanceType;
			typedef typename parallel_safe_partitioned_sssp_msg_value<DistanceType, VertexDescriptor>::msg_type internal_msg_type;
			
			
			
			typedef typename graph_traits<Graph>::degree_size_type degree_size_type;
			
			typedef typename std::vector<std::vector<VertexDescriptor> >::iterator VertexListIterator;
			typedef iterator_property_map<VertexListIterator, IndexMap> PredMapType;
			
			
			typedef typename std::vector<degree_size_type>::iterator DegreeIterator;
			typedef iterator_property_map<DegreeIterator, IndexMap> PathCountMapType;

			typedef typename std::vector<float>::iterator DeltaIterator;
			typedef iterator_property_map<DeltaIterator, IndexMap> DeltaMapType;
			
			sssp_property_maps_for_one_source<DistanceMapType, PredMapType, PathCountMapType, PredMapType, DeltaMapType>* perSourceProps;
			perSourceProps = get_sssp_prop_map_for_source<DistanceMapType, PredMapType, PathCountMapType, DeltaMapType, IndexMap>(sssp_src_global_id, index_map);
			
			
			BGL_FORALL_VERTICES_T(current_vertex, g, Graph) {

						// Default all distances to infinity
						std::vector<VertexDescriptor> preds = perSourceProps->get_predecessors(current_vertex);
						
						typename std::vector<VertexDescriptor>::iterator itr;
						for(itr = preds.begin(); itr != preds.end(); ++itr)
						{
							//std::cout<<get_global_id(*itr)<<",";
							if(owner(*itr) == process_id(process_group(g)))
							{
//								if(sssp_src_global_id == 1)
//								{
//									std::cout<<"Locally appending successor vertex:"<<get_global_id(current_vertex)<<" to:"<<get_global_id(*itr)<<std::endl;
//								}
								bool sappened = perSourceProps->append_succ_if_not_exists(*itr, current_vertex);								
							}
							else
							{
								typename parallel_safe_partioned_successor_msg_value<VertexDescriptor>::msg_type msg = 
								parallel_safe_partioned_successor_msg_value<VertexDescriptor>::create(*itr, current_vertex, v);
								
								//send message here.. 
								int to_send_process = owner(*itr);
								//std::cout<<"sending from v:"<<get_global_id(u)<<" to:"<<get_global_id(v)<<" dist:"<<d<<" wssssps:"<<get_global_id(sssp_source)<<std::endl;
								send(pg, to_send_process, successor_message, msg);
								
//								if(sssp_src_global_id == 1)
//								{
//									std::cout<<"sending successor msg to vertex:"<<get_global_id(*itr)<<" from:"<<get_global_id(current_vertex)<<std::endl;
//								}
							}
						}
						//std::cout<<std::endl;
						// Default all vertex predecessors to the vertex itself
						// put(predecessor_map, current_vertex, current_vertex);
			}
		}
		
		void do_synchronize()
		{
			synchronize(pg);
		}
		
		template<typename message_type, typename VertexDescriptor, typename MsgMap, typename IndexMap>
		bool add_to_distance_message_map(message_type data, IndexMap index_map, MsgMap& msgMap)
		{
				typedef iterator_property_map<std::vector<int>::iterator, IndexMap> DistanceMapType;
				typedef typename property_traits<DistanceMapType>::value_type DistanceType;
				typedef typename parallel_safe_partitioned_sssp_msg_value<DistanceType, VertexDescriptor>::msg_type internal_msg_type;
				
				typedef typename graph_traits<Graph>::vertices_size_type vertices_size_type;
				
				typedef typename graph_traits<Graph>::degree_size_type degree_size_type;
				
				typedef typename std::vector<std::vector<VertexDescriptor> >::iterator VertexListIterator;
				typedef iterator_property_map<VertexListIterator, IndexMap> PredMapType;
				
				
				typedef typename std::vector<degree_size_type>::iterator DegreeIterator;
				typedef iterator_property_map<DegreeIterator, IndexMap> PathCountMapType;
				
				typedef typename std::vector<float>::iterator DeltaIterator;
				typedef iterator_property_map<DeltaIterator, IndexMap> DeltaMapType;
				
				typedef std::pair<VertexDescriptor, 
					internal_msg_type> distance_message_type;
			
				distance_message_type new_value;

				receive(pg, data.first, data.second, new_value);
			
				VertexDescriptor target_vertex = new_value.first; 
				internal_msg_type sssp_msg = new_value.second;
				/*   first          second.first   second.second.first    second.second.second
				 * 	(distance_to_v, (u,            (distance_to_u,        sssp_source))) 
				 */
				
				DistanceType distance_to_target_through_prev = sssp_msg.first;
				VertexDescriptor prev_vertex  = sssp_msg.second.first;
				DistanceType distance_to_prev = sssp_msg.second.second.first;
				DistanceType path_count_to_prev = sssp_msg.second.second.second.first;
				VertexDescriptor main_source  = sssp_msg.second.second.second.second;
				
				vertices_size_type sssp_src_global_id = get_global_id(main_source);

				//std::cout<<"Mypid:"<<pid<<" Recieved for v:"<<get_global_id(target_vertex)<<" from v:"<<get_global_id(prev_vertex)
				//	<<":"<<distance_to_target_through_prev<<" wsssps:"<<sssp_src_global_id<<std::endl;

				//get the properties
				sssp_property_maps_for_one_source<DistanceMapType, PredMapType, PathCountMapType, PredMapType, DeltaMapType>* perSourceProps;
			
			  perSourceProps = get_sssp_prop_map_for_source<DistanceMapType, PredMapType, PathCountMapType, DeltaMapType, IndexMap>(sssp_src_global_id, index_map);


				//cache distance to the prev vertex so as to avoid an unnesessary msg send
				perSourceProps->cache_distance(prev_vertex, distance_to_prev);

				DistanceType currentMinDistanceToTarget = perSourceProps->get_distance(target_vertex);
				
				/*remote_distance_struct<VertexDescriptor, int> remoteS;
				remoteS.target = target_vertex;
				remoteS.dist_to_target = distance_to_target_through_prev;
				
				remoteS.prev = prev_vertex;
				remoteS.dist_to_prev = distance_to_prev;
				
				remoteS.pc_to_prev = path_count_to_prev;*/
				
				typedef std::less<DistanceType> DistanceCompare;
				DistanceCompare distance_compare;
				
				if(distance_compare(currentMinDistanceToTarget, distance_to_target_through_prev))
				{
						return false;
				}
				/*else if(distance_compare(distance_to_target_through_prev, currentMinDistanceToTarget))
				{
					msgMap[main_source].push_back(remoteS);
					return true;	
				}
				else //equal
				{
					bool appended = perSourceProps->append_pred_if_not_exists(target_vertex, prev_vertex);	
					return false;
				}*/
				
				bool appended = false;
				if(distance_compare(distance_to_target_through_prev, currentMinDistanceToTarget))
				{
					perSourceProps->put_distance(target_vertex, distance_to_target_through_prev);
					perSourceProps->clear_pred_list_and_add_one(target_vertex, prev_vertex);
					appended = true;
				}
				else
				{
					//std::cout<<"HERE2:"<<initial_value<<"old:"<<perSourceProps->get_distance(local_source)<<std::endl;
					perSourceProps->append_pred_if_not_exists(target_vertex, prev_vertex);	
				}
				
				
				typedef typename MsgMap::mapped_type VertexQueuePointer;
				typedef indirect_cmp<DistanceMapType, DistanceCompare> DistanceIndirectCompare;
				
				DistanceIndirectCompare distance_indirect_compare(perSourceProps->get_distance_map(), distance_compare);
				
				typedef relaxed_heap<VertexDescriptor, DistanceIndirectCompare, IndexMap> VertexQueue;

				VertexQueuePointer vq = msgMap[main_source];
				if(vq == NULL)
				{
					vq = new VertexQueue(num_vertices(g),
						 distance_indirect_compare,
						 index_map);
					msgMap[main_source] = vq;
				}
				
				if(appended)
				{
					if(vq->contains(target_vertex))
					{
						vq->update(target_vertex);
					}
					else
					{
						vq->push(target_vertex);
					}
					return true;
				}
				
				return false;
		}
		
		template<typename msgMapType, typename IndexMap>
		void process_distance_messages(IndexMap index_map, msgMapType msgMap)
		{
				typedef typename graph_traits<Graph>::vertex_descriptor VertexDescriptor;
				
				typedef typename graph_traits<Graph>::vertices_size_type vertices_size_type;
				
				typedef iterator_property_map<std::vector<int>::iterator, IndexMap> DistanceMapType;
				typedef typename property_traits<DistanceMapType>::value_type DistanceType;

				typedef std::less<DistanceType> DistanceCompare;
				DistanceCompare distance_compare;
		
				typedef typename std::vector<std::vector<VertexDescriptor> >::iterator VertexListIterator;
				typedef iterator_property_map<VertexListIterator, IndexMap> PredMapType;
				
				typedef typename graph_traits<Graph>::degree_size_type degree_size_type;
				typedef typename std::vector<degree_size_type>::iterator DegreeIterator;
				typedef iterator_property_map<DegreeIterator, IndexMap> PathCountMapType;
				
				typedef typename std::vector<float>::iterator DeltaIterator;
				typedef iterator_property_map<DeltaIterator, IndexMap> DeltaMapType;
				
				typename msgMapType::iterator mitr = msgMap.begin();
				typedef typename msgMapType::mapped_type mstruct_type_v;
			
				typedef indirect_cmp<DistanceMapType, DistanceCompare> DistanceIndirectCompare;
				
				typedef relaxed_heap<VertexDescriptor, DistanceIndirectCompare, IndexMap> VertexQueue;
				
				//std::cout<<"pulling:"<<msgMap.size()<<std::endl;
				
				int i = 0;
				int N = msgMap.size();
				sssp_property_maps_for_one_source<DistanceMapType, PredMapType, PathCountMapType, PredMapType, DeltaMapType>* perSourceProps;
				VertexDescriptor main_source;
				
				std::vector<VertexDescriptor> vsources(msgMap.size());
				
				for(i=0, mitr = msgMap.begin(); mitr != msgMap.end(); ++mitr, ++i)
				{
						//add keys to a vector;
						vsources[i] = mitr->first;
						perSourceProps = get_sssp_prop_map_for_source<DistanceMapType, PredMapType, PathCountMapType, DeltaMapType, IndexMap>
											(get_global_id(mitr->first), index_map); //just to create so that we dont have conflict in the parallel section
				}

				
				int tid;
				#pragma omp parallel shared(msgMap, mitr, N, index_map, distance_compare, vsources) private(i, perSourceProps, main_source)
				{
					tid = omp_get_thread_num();
					//std::cout<<"th id"<<tid<<std::endl;
					
					#pragma omp for schedule(static)
					for(i = 0; i < N; i++) //can be done in parallel
					{

						main_source = vsources[i];
				
						perSourceProps = get_sssp_prop_map_for_source<DistanceMapType, PredMapType, PathCountMapType, DeltaMapType, IndexMap>
											(get_global_id(main_source), index_map);
				
						int sssp_updates = parallel_safe_local_sssp(
							main_source,					
							*(msgMap[vsources[i]]),
							index_map,
							perSourceProps,
							distance_compare,
							get(boost::edge_weight, g),
							tid
						);
					}				
				} /* end of parallel regioin */
		}
		
		template<typename message_type, typename VertexDescriptor, typename IndexMap>
		void process_succesor_message(message_type data, IndexMap index_map)
		{			
			typedef typename parallel_safe_partioned_successor_msg_value<VertexDescriptor>::msg_type succ_message_type;
			
			typedef iterator_property_map<std::vector<int>::iterator, IndexMap> DistanceMapType;
			typedef typename property_traits<DistanceMapType>::value_type DistanceType;
			
			
			typedef typename graph_traits<Graph>::degree_size_type degree_size_type;
			
			typedef typename std::vector<std::vector<VertexDescriptor> >::iterator VertexListIterator;
			typedef iterator_property_map<VertexListIterator, IndexMap> PredMapType;
			
			
			typedef typename std::vector<degree_size_type>::iterator DegreeIterator;
			typedef iterator_property_map<DegreeIterator, IndexMap> PathCountMapType;

			typedef typename std::vector<float>::iterator DeltaIterator;
			typedef iterator_property_map<DeltaIterator, IndexMap> DeltaMapType;
				
			succ_message_type new_value;
			
			receive(pg, data.first, data.second, new_value);
			
			VertexDescriptor v = new_value.first;
			VertexDescriptor succ = new_value.second.first;
			VertexDescriptor main_source = new_value.second.second;
			
			int sssp_src_global_id = get_global_id(main_source);
			
			
			sssp_property_maps_for_one_source<DistanceMapType, PredMapType, PathCountMapType, PredMapType, DeltaMapType>* perSourceProps;
			
			perSourceProps = get_sssp_prop_map_for_source<DistanceMapType, PredMapType, PathCountMapType, DeltaMapType, IndexMap>(sssp_src_global_id, index_map);
			
			//std::cout<<"v:"<<get_global_id(v)<<" s:"<<get_global_id(succ)<<std::endl;
			perSourceProps->append_succ_if_not_exists(v, succ);
		}
		
		template<typename SuccQueue, typename VertexDescriptor, typename SRCProps>
		void process_betweenness_reverse_pred(SuccQueue succ_queue, VertexDescriptor main_source, SRCProps perSourceProps)
		{
			
			int sssp_src_global_id = get_global_id(main_source);
			
			while(!succ_queue.empty())
			{
				const VertexDescriptor w = succ_queue.front();				
				
				succ_queue.pop();
				
				int sigmaw = perSourceProps->get_path_count(w);
				float delw = perSourceProps->get_delta(w);
				
				
				
				if(get_global_id(w) != sssp_src_global_id)
				{
					float wcentrality = get(centrality_map, w) + delw;
					//if(delw != 0) std::cout<<"x:"<<delw<<",";
					
//					if(get_global_id(w) == 5)
//					{
//						std::cout<<wcentrality<<":should happen only once;"<<delw<<std::endl;
//					}
					local_put(centrality_map, w, wcentrality);
					/*if(get(centrality_map, w) != 0) 
						std::cout<<"y:"<<get(centrality_map, w)<<std::endl; 
					else if(delw != 0)
						std::cout<<"z"<<std::endl;*/
				}
				
				std::vector<VertexDescriptor> preds = perSourceProps->get_predecessors(w);
				
				typename std::vector<VertexDescriptor>::iterator iv;
				for(iv = preds.begin(); iv != preds.end(); ++iv)
				{
					//std::cout<<get_global_id(*itr)<<",";
					if(owner(*iv) == process_id(process_group(g)))
					{											
						int sigmav = perSourceProps->get_path_count(*iv);
						float delv = perSourceProps->get_delta(*iv);
						
						delv += ((float)sigmav/(float)sigmaw)*(1 + delw);
						
						perSourceProps->set_delta(*iv, delv);
						
						//remove this successor from *iv.
						//we have to wait for all successors to finish their accumulation.
						//std::vector<VertexDescriptor> succ = perSourceProps->get_successors(*iv);
						
						perSourceProps->remove_successor(*iv, w);

						std::vector<VertexDescriptor> ivsucc = perSourceProps->get_successors(*iv);
						
						if(ivsucc.size() == 0)
						{
							succ_queue.push(*iv);
						}
					}
					else
					{
						typedef typename parallel_safe_partioned_between_msg_value<VertexDescriptor, int, float>::msg_type betwn_msg_type;
						
						betwn_msg_type msg = parallel_safe_partioned_between_msg_value<VertexDescriptor, int, float>::create(*iv, w, sigmaw, delw, main_source);
						//send message
						
						int to_send_process = owner(*iv);
								//std::cout<<"sending from v:"<<get_global_id(u)<<" to:"<<get_global_id(v)<<" dist:"<<d<<" wssssps:"<<get_global_id(sssp_source)<<std::endl;
						send(pg, to_send_process, betwen_message, msg);
					}
				}
			}
		}
		
		
		template<typename VertexDescriptor>
		struct remote_betwn_struct
		{
//			VertexDescriptor target;
//			distance_type dist_to_target;
//			VertexDescriptor prev;
//			distance_type dist_to_prev;
//			distance_type pc_to_prev;
			
			VertexDescriptor v;//           = new_value.first;
			int sigmaw;//                   = new_value.second.first;
			float deltaw;//                 = new_value.second.second.first;
			VertexDescriptor w;//           = new_value.second.second.second.first;
			//VertexDescriptor main_source;// = new_value.second.second.second.second;
		};
		
		template<typename message_type, typename VertexDescriptor, typename MsgMap, typename IndexMap>
		void add_to_remote_betwn_msg_map(message_type data, IndexMap index_map, MsgMap& msgMap)
		{
				
				typedef typename parallel_safe_partioned_between_msg_value<VertexDescriptor, int, float>::msg_type between_message_type;
				
				typedef iterator_property_map<std::vector<int>::iterator, IndexMap> DistanceMapType;
				typedef typename property_traits<DistanceMapType>::value_type DistanceType;
				
				
				typedef typename graph_traits<Graph>::degree_size_type degree_size_type;
				
				typedef typename std::vector<std::vector<VertexDescriptor> >::iterator VertexListIterator;
				typedef iterator_property_map<VertexListIterator, IndexMap> PredMapType;
				
				
				typedef typename std::vector<degree_size_type>::iterator DegreeIterator;
				typedef iterator_property_map<DegreeIterator, IndexMap> PathCountMapType;

				typedef typename std::vector<float>::iterator DeltaIterator;
				typedef iterator_property_map<DeltaIterator, IndexMap> DeltaMapType;
					
				between_message_type new_value;
				
				receive(pg, data.first, data.second, new_value);
				
				//std::make_pair(v, std::make_pair(mysigma, std::make_pair(mydelta, sssp_source));
				
				VertexDescriptor v           = new_value.first;
				int sigmaw                   = new_value.second.first;
				float deltaw                 = new_value.second.second.first;
				VertexDescriptor w           = new_value.second.second.second.first;
				VertexDescriptor main_source = new_value.second.second.second.second;
				
				int sssp_src_global_id = get_global_id(main_source);
				
				
				sssp_property_maps_for_one_source<DistanceMapType, PredMapType, PathCountMapType, PredMapType, DeltaMapType>* perSourceProps;
				
				perSourceProps = get_sssp_prop_map_for_source<DistanceMapType, PredMapType, PathCountMapType, DeltaMapType, IndexMap>(sssp_src_global_id, index_map);
			
				remote_betwn_struct<VertexDescriptor> remoteS;
				remoteS.v = v;
				remoteS.sigmaw = sigmaw;
				remoteS.deltaw = deltaw;
				remoteS.w = w;
				
				msgMap[main_source].push_back(remoteS);
		}
		
		template<typename msgMapType, typename IndexMap>
		void process_between_messages(IndexMap index_map, msgMapType msgMap)
		{			
			
			typedef typename graph_traits<Graph>::vertex_descriptor VertexDescriptor;
			typedef typename graph_traits<Graph>::vertex_descriptor VertexDescriptor;
				
			typedef iterator_property_map<std::vector<int>::iterator, IndexMap> DistanceMapType;
			typedef typename property_traits<DistanceMapType>::value_type DistanceType;

			typedef std::less<DistanceType> DistanceCompare;
	
			typedef typename std::vector<std::vector<VertexDescriptor> >::iterator VertexListIterator;
			typedef iterator_property_map<VertexListIterator, IndexMap> PredMapType;
			
			typedef typename graph_traits<Graph>::degree_size_type degree_size_type;
			typedef typename std::vector<degree_size_type>::iterator DegreeIterator;
			typedef iterator_property_map<DegreeIterator, IndexMap> PathCountMapType;
			
			typedef typename std::vector<float>::iterator DeltaIterator;
			typedef iterator_property_map<DeltaIterator, IndexMap> DeltaMapType;
			
			
			typename msgMapType::iterator mitr;
			typedef typename msgMapType::mapped_type mstruct_type_v;
				
			for(mitr = msgMap.begin(); mitr != msgMap.end(); ++mitr)
			{
				VertexDescriptor main_source = mitr->first;
				mstruct_type_v mstruct_v = mitr->second;
				
				typename mstruct_type_v::iterator vitr;
				typedef typename mstruct_type_v::value_type mstruct_type;
				
				int sssp_src_global_id = get_global_id(main_source);
				
				sssp_property_maps_for_one_source<DistanceMapType, PredMapType, PathCountMapType, PredMapType, DeltaMapType>* perSourceProps;
		
				perSourceProps = get_sssp_prop_map_for_source<DistanceMapType, PredMapType, PathCountMapType, DeltaMapType, IndexMap>(sssp_src_global_id, index_map);
		
				std::queue<VertexDescriptor> succ_queue;
			
				for(vitr = mstruct_v.begin(); vitr != mstruct_v.end(); ++vitr)
				{
					int sigmav = perSourceProps->get_path_count((*vitr).v);
					float delv = perSourceProps->get_delta((*vitr).v);
					delv += ((float)sigmav) / (float)((*vitr).sigmaw) * (1 + (*vitr).deltaw);			
					perSourceProps->set_delta((*vitr).v, delv);
					perSourceProps->remove_successor((*vitr).v, (*vitr).w);		
					std::vector<VertexDescriptor> ivsucc = perSourceProps->get_successors((*vitr).v);
					if(ivsucc.size() == 0)
					{
						succ_queue.push((*vitr).v);
					}		
				}
				
				process_betweenness_reverse_pred(succ_queue, main_source, perSourceProps);
			}
		}
		
		template<typename message_type, typename VertexDescriptor, typename IndexMap>
		void process_path_count_message(message_type data, IndexMap index_map)
		{
			typedef typename graph_traits<Graph>::vertex_descriptor VertexDescriptor;
			typedef typename graph_traits<Graph>::vertex_descriptor VertexDescriptor;
				
			typedef iterator_property_map<std::vector<int>::iterator, IndexMap> DistanceMapType;
			typedef typename property_traits<DistanceMapType>::value_type DistanceType;

			typedef std::less<DistanceType> DistanceCompare;
	
			typedef typename std::vector<std::vector<VertexDescriptor> >::iterator VertexListIterator;
			typedef iterator_property_map<VertexListIterator, IndexMap> PredMapType;
			
			typedef typename graph_traits<Graph>::degree_size_type degree_size_type;
			typedef typename std::vector<degree_size_type>::iterator DegreeIterator;
			typedef iterator_property_map<DegreeIterator, IndexMap> PathCountMapType;
			
			typedef typename std::vector<float>::iterator DeltaIterator;
			typedef iterator_property_map<DeltaIterator, IndexMap> DeltaMapType;
			
			
			typedef typename parallel_safe_partioned_path_count_msg_value<VertexDescriptor, unsigned int>::msg_type pc_message_type;
			
			pc_message_type new_value;
			
			//std::cout<<"I AM HERE 1"<<std::endl;
			
			receive(pg, data.first, data.second, new_value);
			
			//std::cout<<"I AM HERE 2"<<std::endl;
			//return;
			//return std::make_pair(v, std::make_pair(self, std::make_pair(pred_cnt, ssspsource)));
			
			VertexDescriptor v           = new_value.first;
			VertexDescriptor pred        = new_value.second.first;
			unsigned int path_cnt    = new_value.second.second.first;
			VertexDescriptor main_source = new_value.second.second.second;
			
			int sssp_src_global_id = get_global_id(main_source);
	
			sssp_property_maps_for_one_source<DistanceMapType, PredMapType, PathCountMapType, PredMapType, DeltaMapType>* perSourceProps;
		
			perSourceProps = get_sssp_prop_map_for_source<DistanceMapType, PredMapType, PathCountMapType, DeltaMapType, IndexMap>(sssp_src_global_id, index_map);
		
			std::queue<VertexDescriptor> pred_queue;
			
			perSourceProps->increment_path_count(v, path_cnt);
			
			perSourceProps->decrement_pred_count(v);

			unsigned int pred_count = perSourceProps->get_pred_count(v);
			
			if(pred_count == 0)
			{
				pred_queue.push(v);
			}
			//if(get_global_id(main_source) == 1) std::cout<<"Received from:"<<get_global_id(pred)<<" for:"<<get_global_id(v)<<std::endl;
			//std::cout<<"I AM HERE"<<std::endl;
			process_path_count_forward_succ(pred_queue, main_source, perSourceProps);
		}
		
		template<typename IndexMap>
		int process_messages(IndexMap index_map) //dummy parameters just to get the template types
		{
			typedef typename graph_traits<Graph>::vertex_descriptor VertexDescriptor;
			
			int num_messages = 0;
			typedef std::pair<int, int> message_type;
			
			typedef typename graph_traits<Graph>::vertices_size_type vertices_size_type;
			
			typedef iterator_property_map<std::vector<int>::iterator, IndexMap> DistanceMapType;
			typedef typename property_traits<DistanceMapType>::value_type DistanceType;

			typedef std::less<DistanceType> DistanceCompare;
			DistanceCompare distance_compare;
			
			typedef indirect_cmp<DistanceMapType, DistanceCompare> DistanceIndirectCompare;

			typedef relaxed_heap<VertexDescriptor, DistanceIndirectCompare, IndexMap> VertexQueue;				
			
			typedef std::map<VertexDescriptor, VertexQueue* >	msgMap_type;
			
			typedef std::map<VertexDescriptor, std::vector<remote_betwn_struct<VertexDescriptor> > >	betwnMsgMap_type;
			
			msgMap_type msgMap;
			betwnMsgMap_type betwnMsgMap;
			
			bool dist_msg = false;
			bool betwn_msg = false;
			
			struct timeval start_time; 
			struct timeval end_time;

				
			gettimeofday(&start_time, NULL);
			
			while (optional<message_type> msg = probe(pg)) {
				num_messages++;
				
				message_type data = msg.get();
				
				if(data.second == distance_message)
				{
					
					//template<typename message_type, typename VertexDescriptor, typename IndexMap>
					//void add_to_distance_message_map(message_type data, IndexMap index_map, std::map<VertexDescriptor, std::vector<remote_distance_struct<VertexDescriptor, int> > > msgMap)
					
					if(add_to_distance_message_map<message_type, VertexDescriptor, msgMap_type, IndexMap>(data, index_map, msgMap))
					/*{
						num_messages++;
					}*/
					
					dist_msg = true;
				}
				else if(data.second == successor_message)
				{
					process_succesor_message<message_type, VertexDescriptor, IndexMap>(data, index_map);
				}
				else if(data.second == betwen_message)
				{
					//process_between_message<message_type, VertexDescriptor, IndexMap>(data, index_map);
					add_to_remote_betwn_msg_map<message_type, VertexDescriptor, betwnMsgMap_type, IndexMap>(data, index_map, betwnMsgMap);
					betwn_msg = true;
				}
				else if(data.second == path_count_message)
				{
					//std::cout<<"I AM HERE";
					process_path_count_message<message_type, VertexDescriptor, IndexMap>(data, index_map);
				}
			}
			
			if(dist_msg)
			{
				
				gettimeofday(&end_time, NULL); 

				float duration = (end_time.tv_sec - start_time.tv_sec) + (end_time.tv_usec - start_time.tv_usec) * 1E-6;
				//std::cout<<g.processor()<<": reading messages SSSP: "<< num_messages <<" : " << duration<<std::endl;
				
				
				gettimeofday(&start_time, NULL);
				
				process_distance_messages(index_map, msgMap);
				
				gettimeofday(&end_time, NULL); 

				duration = (end_time.tv_sec - start_time.tv_sec) + (end_time.tv_usec - start_time.tv_usec) * 1E-6;
				//std::cout<<g.processor()<<": processing distance messages SSSP: "<< duration<<std::endl;
				
				msgMap.clear(); //TODO: DELETE QUEUE OBJECTS
			}
			else if(betwn_msg)
			{
				process_between_messages(index_map, betwnMsgMap);
			}
			return num_messages;
		}
		
		
		void printCentrality()
		{
			 //std::cout<<"**********centralities************"<<std::endl;
			 BGL_FORALL_VERTICES_T(current_vertex, g, Graph) {
				//std::cout<<process_id(pg)<<" v:"<<get_global_id(current_vertex)<<" C:"<<get(centrality_map, current_vertex)<<std::endl;
				if(get(centrality_map, current_vertex) != 0)
				{
					//std::cout<<pid<<":"<<get_global_id(current_vertex)<<":"<<get(centrality_map, current_vertex)<<std::endl;
					std::cout<<get_global_id(current_vertex)<<":"<<get(centrality_map, current_vertex)<<std::endl;
				}
			 }
		}
		template<typename IndexMap>
		void printDetails(IndexMap index_map)
		{
				
			typedef typename graph_traits<Graph>::vertices_size_type vertices_size_type;	
			 std::map<vertices_size_type, sssp_property_maps_for_one_source_base*> all_source_props = get_allsource_sssp_prop_map();
			 typename std::map<vertices_size_type, sssp_property_maps_for_one_source_base*>::iterator itr = all_source_props.begin();
			 for(;itr != all_source_props.end(); ++itr)
			 {
				 printDetails(vertex((*itr).first, g), (*itr).first, index_map);
			 }
		}
		
		template<typename VertexDescriptor, typename IndexMap>
		void printDetails(VertexDescriptor v, int sssp_src_global_id, IndexMap index_map)
		{
				typedef iterator_property_map<std::vector<int>::iterator, IndexMap> DistanceMapType;
				typedef typename property_traits<DistanceMapType>::value_type DistanceType;
				typedef typename parallel_safe_partitioned_sssp_msg_value<DistanceType, VertexDescriptor>::msg_type internal_msg_type;
				
				
				std::ostringstream outfile;
				outfile << "/home/kumbhare/F_Drive/";
				outfile << process_id(process_group(g))<<"_";
				outfile << sssp_src_global_id;
				outfile << "_2.txt";
				std::cout<<"PRINTING TO FILE for src:"<<sssp_src_global_id<<":"<<outfile.str()<<std::endl;
				std::ofstream dof(outfile.str().c_str());
				
				typedef typename graph_traits<Graph>::degree_size_type degree_size_type;
				
				typedef typename std::vector<std::vector<VertexDescriptor> >::iterator VertexListIterator;
				typedef iterator_property_map<VertexListIterator, IndexMap> PredMapType;
				
				
				typedef typename std::vector<degree_size_type>::iterator DegreeIterator;
				typedef iterator_property_map<DegreeIterator, IndexMap> PathCountMapType;
				
				typedef typename std::vector<float>::iterator DeltaIterator;
				typedef iterator_property_map<DeltaIterator, IndexMap> DeltaMapType;
				
				sssp_property_maps_for_one_source<DistanceMapType, PredMapType, PathCountMapType, PredMapType, DeltaMapType>* perSourceProps;
			
			  perSourceProps = get_sssp_prop_map_for_source<DistanceMapType, PredMapType, PathCountMapType, DeltaMapType, IndexMap>(sssp_src_global_id, index_map);
				
				BGL_FORALL_VERTICES_T(current_vertex, g, Graph) {
						// Default all distances to infinity
						
						DistanceType dist = perSourceProps->get_distance(current_vertex);
						degree_size_type pathCount = perSourceProps->get_path_count(current_vertex);
						std::vector<VertexDescriptor> preds = perSourceProps->get_predecessors(current_vertex);
						
						//std::cout<<process_id(pg)<<"src:"<<sssp_src_global_id<<" v:"<<get_global_id(current_vertex)<<" d:"<<dist<<" pc:"<<pathCount<<" pre:";
						dof<<get_global_id(current_vertex)<<":"<<dist<<":"<<pathCount<<":pred:"<<perSourceProps->get_pred_count(current_vertex);
						
						dof<<":p:";
						typename std::vector<VertexDescriptor>::iterator itr;
						for(itr = preds.begin(); itr != preds.end(); ++itr)
						{
							//std::cout<<get_global_id(*itr)<<",";
							dof<<get_global_id(*itr)<<"*";
						}
						
						std::vector<VertexDescriptor> succs = perSourceProps->get_successors(current_vertex);
						dof<<":s:";
						typename std::vector<VertexDescriptor>::iterator sitr;
						for(sitr = succs.begin(); sitr != succs.end(); ++sitr)
						{
							dof<<get_global_id(*sitr)<<"*";
						}
						dof<<std::endl;
						//std::cout<<std::endl;
						// Default all vertex predecessors to the vertex itself
						// put(predecessor_map, current_vertex, current_vertex);
				}	
				
				dof.close();
		}
		
		template<typename Iter, typename CentralityMap>
		inline void divide_centrality_by_two(std::pair<Iter, Iter> keys,
                           CentralityMap centrality_map)
		{
			typename property_traits<CentralityMap>::value_type two(2);
			while (keys.first != keys.second) {
				put(centrality_map, *keys.first, get(centrality_map, *keys.first) / two);
				++keys.first;
			}
		}

		void divide_centrality_by_two()
		{			
			typedef typename graph_traits<Graph>::directed_category directed_category;
			const bool is_undirected = 
				is_convertible<directed_category*, undirected_tag*>::value;
			if (is_undirected) {
				divide_centrality_by_two(vertices(g), centrality_map);
			}
		}
		
		template<typename CentralityMap, typename IndexMap, typename VertexDescriptor>
		void dispatch_in_parallel(Graph &g, CentralityMap cm, IndexMap index_map, VertexDescriptor sssp_source)
		{
			
			typedef typename graph_traits<Graph>::vertex_iterator vertex_iterator;
			
			typedef iterator_property_map<std::vector<int>::iterator, IndexMap> DistanceMapType;
			typedef typename property_traits<DistanceMapType>::value_type DistanceType;

			typedef std::less<DistanceType> DistanceCompare;

			typedef typename std::vector<std::vector<VertexDescriptor> >::iterator VertexListIterator;
			typedef iterator_property_map<VertexListIterator, IndexMap> PredMapType;
			
			typedef typename graph_traits<Graph>::degree_size_type degree_size_type;
			typedef typename std::vector<degree_size_type>::iterator DegreeIterator;
			typedef iterator_property_map<DegreeIterator, IndexMap> PathCountMapType;
			
			typedef typename std::vector<float>::iterator DeltaIterator;
			typedef iterator_property_map<DeltaIterator, IndexMap> DeltaMapType;
			
			DistanceCompare distance_compare;

			typedef indirect_cmp<DistanceMapType, DistanceCompare> DistanceIndirectCompare;

			typedef relaxed_heap<VertexDescriptor, DistanceIndirectCompare, IndexMap> VertexQueue;

			sssp_property_maps_for_one_source<DistanceMapType, PredMapType, PathCountMapType, PredMapType, DeltaMapType>* perSourceProps;

			//TODO MOVE THIS HIGER
			if(owner(sssp_source) != process_id(pg))
			{
				return;
			}
		
			int sssp_src_global_id = get_global_id(sssp_source);
			
			perSourceProps = get_sssp_prop_map_for_source<DistanceMapType, PredMapType, PathCountMapType, DeltaMapType, IndexMap>(sssp_src_global_id, index_map);
			
			
			//SAMPLE GET DISTANCE SHOULD BE INFINITE.. NOT ZERO
			//std::cout<<"SAMPLE D TO VERTEX 0:"<<perSourceProps->get_distance(vertex(0,g))<<std::endl;
				
			DistanceIndirectCompare distance_indirect_compare(perSourceProps->get_distance_map(), distance_compare);
				
			VertexQueue vertex_queue(num_vertices(g),
							 distance_indirect_compare,
							 index_map);	
				
			perSourceProps->put_distance(sssp_source, DistanceType());
			
			perSourceProps->set_path_count(sssp_source, 1);

			vertex_queue.push(sssp_source);
			
			int sssp_updates = parallel_safe_local_sssp(
						sssp_source,					
						vertex_queue,
						index_map,
						perSourceProps,
						distance_compare,
						get(boost::edge_weight, g)
			);
		}
		
		
		template<typename IndexMap>
		int calculate_all_path_counts(IndexMap index_map)
		{
			 typedef typename graph_traits<Graph>::vertices_size_type vertices_size_type;	
			 std::map<vertices_size_type, sssp_property_maps_for_one_source_base*> all_source_props = get_allsource_sssp_prop_map();
			 typename std::map<vertices_size_type, sssp_property_maps_for_one_source_base*>::iterator itr = all_source_props.begin();
			 
			 for(;itr != all_source_props.end(); ++itr)
			 {
				 //std::cout<<"Calculating for source:"<<(*itr).first<<std::endl;
				 calculate_local_path_counts(vertex((*itr).first, g), index_map);
			 }
		}
		
		template<typename VertexDescriptor, typename IndexMap>
		int calculate_local_path_counts(VertexDescriptor v, IndexMap index_map)
		{
			int updates = 0;
			int sssp_src_global_id = get_global_id(v);
			
			typedef iterator_property_map<std::vector<int>::iterator, IndexMap> DistanceMapType;
			typedef typename property_traits<DistanceMapType>::value_type DistanceType;
			typedef typename parallel_safe_partitioned_sssp_msg_value<DistanceType, VertexDescriptor>::msg_type internal_msg_type;
			
			
			
			typedef typename graph_traits<Graph>::degree_size_type degree_size_type;
			
			typedef typename std::vector<std::vector<VertexDescriptor> >::iterator VertexListIterator;
			typedef iterator_property_map<VertexListIterator, IndexMap> PredMapType;
			
			
			typedef typename std::vector<degree_size_type>::iterator DegreeIterator;
			typedef iterator_property_map<DegreeIterator, IndexMap> PathCountMapType;
			
			typedef typename std::vector<float>::iterator DeltaIterator;
			typedef iterator_property_map<DeltaIterator, IndexMap> DeltaMapType;
			
			sssp_property_maps_for_one_source<DistanceMapType, PredMapType, PathCountMapType, PredMapType, DeltaMapType>* perSourceProps;
			perSourceProps = get_sssp_prop_map_for_source<DistanceMapType, PredMapType, PathCountMapType, DeltaMapType, IndexMap>(sssp_src_global_id, index_map);
			
			
			//put the "source" into the queue
			std::queue<VertexDescriptor> succ_queue;
			
			if(owner(v) == pid)
				succ_queue.push(v);
			
			process_path_count_forward_succ(succ_queue, v, perSourceProps);
		}
	

		template<typename SuccQueue, typename VertexDescriptor, typename SRCProps>
		void process_path_count_forward_succ(SuccQueue succ_queue, VertexDescriptor main_source, SRCProps perSourceProps)
		{
			
			int sssp_src_global_id = get_global_id(main_source);
			
			while(!succ_queue.empty())
			{
				const VertexDescriptor w = succ_queue.front();				
				
				succ_queue.pop();
				
				std::vector<VertexDescriptor> succs = perSourceProps->get_successors(w);
				
				typename std::vector<VertexDescriptor>::iterator iv;
				for(iv = succs.begin(); iv != succs.end(); ++iv)
				{
					//std::cout<<get_global_id(w)<<":s:"<<get_global_id(*iv)<<std::endl;
					if(owner(*iv) == process_id(process_group(g)))
					{											
						perSourceProps->increment_path_count(*iv, perSourceProps->get_path_count(w));
						
						perSourceProps->decrement_pred_count(*iv);

						unsigned int pred_count = perSourceProps->get_pred_count(*iv);
						
						if(pred_count == 0)
						{
							succ_queue.push(*iv);
						}
					}
					else
					{
						typedef typename parallel_safe_partioned_path_count_msg_value<VertexDescriptor, unsigned int>::msg_type path_cnt_msg_type;
						
						path_cnt_msg_type msg = parallel_safe_partioned_path_count_msg_value<VertexDescriptor, unsigned int>::create(*iv, w, perSourceProps->get_path_count(w), 
																																																																				main_source);
						//send message
						
						int to_send_process = owner(*iv);
						//if(sssp_src_global_id==1) std::cout<<"sending from v:"<<get_global_id(w)<<" to:"<<get_global_id(*iv)<<std::endl;
						send(pg, to_send_process, path_count_message, msg);
					}
				}
			}
		}
	};


	boost::random::mt19937 gen(time(NULL));
	//boost::random::mt19937 gen(100);

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
	
	/*template<typename vertices_size_type, typename Graph>
	std::vector<vertices_size_type> randomize_vertex_order(vertices_size_type n, Graph& g)
	{
		
		typedef typename graph_traits<Graph>::vertex_descriptor VertexDescriptor;
		typedef typename graph_traits<Graph>::vertex_iterator vertex_iterator;
		typedef typename graph_traits<Graph>::vertices_size_type vertex_size_t;
		
		
		vertex_iterator v, v_end;
		boost::tie(v, v_end) = vertices(g);
		
		boost::random::uniform_int_distribution<> rand_vertex(0,n-1);
		std::vector<vertices_size_type>	ordered_vertices;
		std::vector<vertices_size_type>	empty_vertices;
		
		vertices_size_type i = 0;
		while(i < n) {
			vertices_size_type vidx = rand_vertex(gen);
			
			if(out_degree(*(v+vidx), g) == 0)
			{
				if(std::find(empty_vertices.begin(), empty_vertices.end(), vidx) == empty_vertices.end()) {
					empty_vertices.push_back(vidx);
					i++;					
				}				
				continue;
			}
			
			if(std::find(ordered_vertices.begin(), ordered_vertices.end(), vidx) == ordered_vertices.end()) {
				ordered_vertices.push_back(vidx);
				i++;
			}
		}
		return ordered_vertices;
	}*/

	
	template<typename Graph, typename CentralityMap, typename IndexMap>
	void partiotined_betweenness_centrality(Graph &g, CentralityMap cm, IndexMap index_map)
	{
 		struct timeval start_time; 
	  struct timeval end_time; 

		
		
		typedef typename graph_traits<Graph>::vertex_descriptor VertexDescriptor;
		typedef typename graph_traits<Graph>::vertex_iterator vertex_iterator;
		typedef typename graph_traits<Graph>::vertices_size_type vertex_size_t;
		
		
			parallel_safe_partitioned_sssp<Graph, CentralityMap> pspsssp(g,cm);
		
			int i = 0;

			vertex_iterator v, v_end;
			boost::tie(v, v_end) = vertices(g);
			
			vertex_size_t n_local_verts = num_vertices(g);
			vertex_size_t vertex_itr_ctr = 0;
			std::vector<vertex_size_t> ordered_vertices = randomize_vertex_order(n_local_verts);
			
			int to_continue = 1;
			int cntr = 0;
			
			while(to_continue > 0)
			{
				gettimeofday(&start_time, NULL);
				int sssp_updates = 0;
				
				
				//run local sssp
				
				//VertexDescriptor x = vertex(19,g);
				if(vertex_itr_ctr < n_local_verts)
				{
					//VertexDescriptor x = *(v+ordered_vertices[vertex_itr_ctr]);
					VertexDescriptor x = vertex(21353,g);
					//std::cout<<g.processor()<<":ss:"<<pspsssp.get_global_id(x)<<owner(x)<<std::endl;
					pspsssp.dispatch_in_parallel(g, cm, index_map, x);				
				}

				do
				{
					pspsssp.do_synchronize();
					sssp_updates = pspsssp.process_messages(get(boost::vertex_index, g));
					sssp_updates = all_reduce(process_group(g), sssp_updates, boost::parallel::sum<int>());
				}while(sssp_updates > 0);
				
				
				
				
				//gather successors
				
				int gather_updates = 0;
				//if(v < v_end) WE SHOULD NOT DO THIS HERE since we might have to gather from other sources
				gather_updates = pspsssp.gather_all_local_successors(get(boost::vertex_index, g));
				
				do
				{
					pspsssp.do_synchronize();
					gather_updates = pspsssp.process_messages(get(boost::vertex_index, g));
					gather_updates = all_reduce(process_group(g), gather_updates, boost::parallel::sum<int>());
				}while(gather_updates > 0);
				
				//WORAROUND FOR THE PATH COUNT BUG
				//RERUN PATH_COUNT COLLECTION.. :(
				//vertex_size_t pc_updates = 0;
				//pc_updates = pspsssp.update_all_pcs(get(boost::vertex_index, g));
				
				int pc_updates = pspsssp.calculate_all_path_counts(get(boost::vertex_index, g));
				do
				{
					pspsssp.do_synchronize();
					pc_updates = pspsssp.process_messages(get(boost::vertex_index, g));
					pc_updates = all_reduce(process_group(g), pc_updates, boost::parallel::sum<int>());
				}while(pc_updates > 0);
				
				
				//calculate local betweenness values
				int betwn_updates = 0;
				//if(v < v_end) WE SHOULD NOT DO THIS HERE since we might have to calculate for other sources
				pspsssp.calculate_all_local_betweenness(get(boost::vertex_index, g));
				
				do{
					pspsssp.do_synchronize();
					betwn_updates = pspsssp.process_messages(get(boost::vertex_index, g));
					//pspsssp.calculate_all_local_betweenness(get(boost::vertex_index, g));
					betwn_updates = all_reduce(process_group(g), betwn_updates, boost::parallel::sum<int>());
				}while(betwn_updates > 0);
				
				
			  //v++;				
				if(vertex_itr_ctr < n_local_verts)
				{
					to_continue = 1;
				}
				else
				{
					to_continue = 0;
				}
				to_continue = all_reduce(process_group(g), to_continue, boost::parallel::sum<int>());

				pspsssp.clear_allsource_sssp_prop_map();
				gettimeofday(&end_time, NULL);
	
				//end add edes
				float data_duration = (end_time.tv_sec - start_time.tv_sec) + (end_time.tv_usec - start_time.tv_usec) * 1E-6;
				//std::cout<<"*"<<vertex_itr_ctr<<":"<<pspsssp.get_global_id(*(v+ordered_vertices[vertex_itr_ctr]))<<":"<<data_duration<<"*";
				//std::cout<<"*"<<pspsssp.get_global_id(*(v+ordered_vertices[vertex_itr_ctr]))<<"*"<<std::endl;
				vertex_itr_ctr++;
				if(vertex_itr_ctr == 1)
					break;
			}
		//ask everyone to print locally
		pspsssp.divide_centrality_by_two();
		//std::cout<<std::endl;
		pspsssp.printCentrality();
		//pspsssp.printDetails(get(vertex_index, g));
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


	
	
	template<typename Graph, typename CentralityMap, typename IndexMap, typename vertex_size_t>
	void partiotined_chong_extract_high_centrality(Graph &g, CentralityMap cm, IndexMap index_map, 
																											vertex_size_t topk, vertex_size_t bsize, vertex_size_t set_delta, vertex_size_t convergence_cnt)
	{
		typedef typename graph_traits<Graph>::vertex_descriptor VertexDescriptor;
		typedef typename graph_traits<Graph>::vertex_iterator vertex_iterator;
		
			parallel_safe_partitioned_sssp<Graph, CentralityMap> pspsssp(g,cm);
		
			typedef typename property_traits<CentralityMap>::value_type centrality_type;
						
			typedef typename std::pair<vertex_size_t, centrality_type> centrality_pair_type;
			
			std::vector<centrality_pair_type> local_max_topn;
			std::vector<centrality_pair_type> aggregated_top_n;
			std::vector<centrality_pair_type> collected_top;
		
			vertex_size_t n_local_verts = num_vertices(g);
			vertex_size_t vertex_itr_ctr = 0;
			std::vector<vertex_size_t> ordered_vertices = randomize_vertex_order(n_local_verts);

			vertex_iterator v, v_end;
			boost::tie(v, v_end) = vertices(g);
			
			vertex_size_t to_continue = 1;

			vertex_size_t pid = process_id(process_group(g));

			vertex_size_t local_paths_calculated_in_current_batch = 0;
			vertex_size_t total_paths_calculated_in_current_batch = 0;
			vertex_size_t converged = 0;
			
			mpi_process_group pg = process_group(g);
			std::set<vertex_size_t> prevSet;
			std::set<vertex_size_t> currSet;
			vertex_size_t stable_count = 0;
			
			vertex_size_t iteration_counter = 0;
			
			struct timeval start_time; 
			struct timeval end_time;
			float duration;
			
			int num_sssps = 0;
			
			while(to_continue > 0)
			{
				
				gettimeofday(&start_time, NULL);
				
				iteration_counter++;
				//std::cout<<pid<<": Total so far:"<<total_paths_calculated_in_current_batch<<std::endl;
				
				vertex_size_t new_updates = 0;
				
				bool run_local_path = false;
				
				//negotiate who will run
				vertex_size_t local_vertices_available = 0;
				if(vertex_itr_ctr < num_vertices(g))
				{
					local_vertices_available = 1;
				}
				
				vertex_size_t total_proc_with_vertices = all_reduce(process_group(g), local_vertices_available, boost::parallel::sum<vertex_size_t>());
				
				vertex_size_t remaining_in_batch = bsize - total_paths_calculated_in_current_batch;
				
				if(remaining_in_batch >= total_proc_with_vertices)
				{
					run_local_path = true;
				}
				else
				{
					//i.e. remaining in batch is less than procs with available local vertices. choose the first n with true.
					
					std::pair<vertex_size_t, vertex_size_t> pid_available_pair = std::make_pair(pid, local_vertices_available);
					std::vector<std::pair<vertex_size_t, vertex_size_t> > local_data;
					local_data.push_back(pid_available_pair);
					
					std::vector<std::pair<vertex_size_t, vertex_size_t> > all_data;
					
					all_gather(process_group(g), local_data.begin(), local_data.end(), all_data);
					
					vertex_size_t before_me = 0;
					for (typename std::vector<std::pair<vertex_size_t, vertex_size_t> >::iterator iter = all_data.begin();
           iter != all_data.end(); ++iter) {
						if(iter->first < pid)
						{
							before_me++;
						}
					}
					
					if(before_me <= pid && before_me < remaining_in_batch)
					{
						run_local_path = true;
					}
					else				
					{
						run_local_path = false;
					}
				}

				
				//run local sssp
				
				//VertexDescriptor x = vertex(1,g);
				if(vertex_itr_ctr < num_vertices(g) && run_local_path)
				{					
					VertexDescriptor x = *(v+ordered_vertices[vertex_itr_ctr]);
					//VertexDescriptor x = vertex(21353, g);
					
					//std::cout<<process_id(process_group(g))<<" ss:"<<pspsssp.get_global_id(x)<<std::endl;
//				std::cout<<g.processor()<<":ss:"<<pspsssp.get_global_id(x)<<":"<< pspsssp.get_global_id(vertex(pspsssp.get_global_id(x), g)) <<std::endl;
					pspsssp.dispatch_in_parallel(g, cm, index_map, x);
					local_paths_calculated_in_current_batch++;
					//v++;
					vertex_itr_ctr++;
				}

//				gettimeofday(&end_time, NULL); 
//
//				float duration = (end_time.tv_sec - start_time.tv_sec) + (end_time.tv_usec - start_time.tv_usec) * 1E-6;
//				std::cout<<g.processor()<<": Local SSSP: "<< duration << ":" <<vertex_itr_ctr<<std::endl;
//				
//				gettimeofday(&start_time, NULL); 
//				pspsssp.do_synchronize();
//				gettimeofday(&end_time, NULL); 
//
//				duration = (end_time.tv_sec - start_time.tv_sec) + (end_time.tv_usec - start_time.tv_usec) * 1E-6;
//				
//				std::cout<<g.processor()<<": Sync time: "<< duration << std::endl;
//				break;		
				
				
				gettimeofday(&start_time, NULL);
				
				do
				{
					pspsssp.do_synchronize();
					new_updates = pspsssp.process_messages(get(boost::vertex_index, g));
					new_updates = all_reduce(process_group(g), new_updates, boost::parallel::sum<vertex_size_t>());					
					//break;
				}while(new_updates > 0);
				
				gettimeofday(&end_time, NULL); 

				duration = (end_time.tv_sec - start_time.tv_sec) + (end_time.tv_usec - start_time.tv_usec) * 1E-6;
				//std::cout<<g.processor()<<": all process msgs SSSP: "<< duration<<std::endl;
				
				/*std::cout<<"Done sssp"<<std::endl;
				pspsssp.printDetails(get(vertex_index, g));
				break;*/
				
				//gather successors				

				//if(v < v_end) WE SHOULD NOT DO THIS HERE since we might have to gather from other sources
				new_updates = pspsssp.gather_all_local_successors(get(boost::vertex_index, g));
				
				pspsssp.do_synchronize();
				new_updates = pspsssp.process_messages(get(boost::vertex_index, g));					


				//std::cout<<"done successors"<<std::endl;
				//pspsssp.printDetails(get(vertex_index, g));
				//break;

				
				new_updates = pspsssp.calculate_all_path_counts(get(boost::vertex_index, g));
				do
				{
					pspsssp.do_synchronize();
					new_updates = pspsssp.process_messages(get(boost::vertex_index, g));
					new_updates = all_reduce(process_group(g), new_updates, boost::parallel::sum<int>());
				}while(new_updates > 0);
				//pspsssp.do_synchronize();
				
				//std::cout<<"Done pathcount"<<std::endl;
				/*pspsssp.printDetails(get(vertex_index, g));
				break;*/

				//if(v < v_end) WE SHOULD NOT DO THIS HERE since we might have to calculate for other sources
				new_updates = pspsssp.calculate_all_local_betweenness(get(boost::vertex_index, g));
				
				do{
					pspsssp.do_synchronize();
					new_updates = pspsssp.process_messages(get(boost::vertex_index, g));
					new_updates = all_reduce(process_group(g), new_updates, boost::parallel::sum<vertex_size_t>());
				}while(new_updates > 0);
				
			  //std::cout<<"Done betwennness"<<std::endl;
				//pspsssp.printDetails(get(vertex_index, g));
				//*pspsssp.printCentrality();
				//break;
				
				
				total_paths_calculated_in_current_batch = all_reduce(process_group(g), local_paths_calculated_in_current_batch, boost::parallel::sum<vertex_size_t>());
				
				//std::cout<<"sssps in current batch:"<<total_paths_calculated_in_current_batch<<std::endl;
				if(total_paths_calculated_in_current_batch == bsize)
				{
					//std::cout<<pid<<"DONE Batch:"<<total_paths_calculated_in_current_batch<<std::endl;
					//if(pid == 0)
					//{
						//std::cout<<"#"<<std::endl;						
					//}
					num_sssps += bsize;
					local_paths_calculated_in_current_batch = 0;
					total_paths_calculated_in_current_batch = 0;
					
					local_max_topn.clear();
								
					vertex_iterator cvi, cv_end;
					for (boost::tie(cvi, cv_end) = vertices(g); cvi != cv_end; ++cvi) {
						centrality_type c_centality = get(cm, *cvi);
						//c_vid is the global id of the vertex
						vertex_size_t c_vid = pspsssp.get_global_id(*cvi);

						add_to_pair_list(local_max_topn, topk, c_vid, c_centality);
					}
					
//					for(vertex_size_t ix = 0; ix < local_max_topn.size(); ix++) {
//								centrality_pair_type cpair = local_max_topn[ix];
//								std::cout<<pid<<":"<<ix<<":"<<cpair.first<<">"<<cpair.second<<std::endl;
//					}
					
					
					//std::cout<<pid<<"DONE local add to pair:"<<total_paths_calculated_in_current_batch<<std::endl;
					
					vertex_size_t done = 0;
					aggregated_top_n.clear();
					
					//for(int topi = 0; topi < local_max_topn.size(); topi++)
					vertex_size_t topi = 0;
					while(done == 0)
					{
						centrality_pair_type to_send;
						if(topi < local_max_topn.size())
						{
							to_send = local_max_topn[topi];
						}
						else
						{
							to_send = std::make_pair(std::numeric_limits<vertex_size_t>::max(),-1);
						}
						
						
						collected_top.clear();
						if(pid == 0)
						{
							gather(communicator(pg), to_send, collected_top, 0);
							
							for(vertex_size_t i = 0; i < collected_top.size(); i++) {
								centrality_pair_type cpair = collected_top[i];
								add_to_pair_list(aggregated_top_n, topk, cpair.first, cpair.second);
							}							
						}
						else
						{
							//gather(pg, local_max_topn[topi], 0);
							gather(communicator(pg), to_send, 0);
						}
							
							
						centrality_pair_type min;
						if(pid == 0)
						{							
							min = aggregated_top_n.back();
						}	
						broadcast(communicator(pg), min, 0);
						
						int incremented = 0;
						if(to_send.second >= min.second && to_send.second != -1)
						{
							topi++;
							incremented = 1;
						}
	
						//std::cout<<pid<<"DONE gather:"<<total_paths_calculated_in_current_batch<<std::endl;
						incremented = all_reduce(pg, incremented, boost::parallel::sum<int>());
						if(incremented == 0)
						{
							break; //break out of 
						}
					}
					
					
					//std::cout<<pid<<"DONE Aggregatoin:"<<total_paths_calculated_in_current_batch<<std::endl;
					
					if(pid == 0)
					{
						currSet.clear();
						for(int i = 0; i < aggregated_top_n.size(); i++) {
							centrality_pair_type cpair = aggregated_top_n[i];
							currSet.insert(cpair.first);
							local_put(cm, vertex(cpair.first,g), cpair.second);
						}
					
						std::vector<vertex_size_t> v_intersection;
						set_intersection(currSet.begin(),currSet.end(),prevSet.begin(),prevSet.end(),
							std::back_inserter(v_intersection));
						
						if((currSet.size() - v_intersection.size()) <= 1) { //? TODO: get the delta count from user
							stable_count++;
						}
						else
						{
							stable_count = 0;	//Check if sotera does the same thing (oi.e. rest counter to 0)
						}
						
						if(stable_count >= convergence_cnt) {
							//std::cout<<pid<<": Constant top set. Stoppping"<<std::endl;
							converged = 1;
						}
					}

					broadcast(communicator(pg), converged, 0);
					
					prevSet.clear();
					prevSet.insert(currSet.begin(),currSet.end());

					//std::cout<<"***HERE2***"<<std::endl;					
					//Break after one batch for now to test perf.. 
					//break;
				}
				
				if(vertex_itr_ctr < num_vertices(g) && converged == 0)
				{
					to_continue = 1;
				}
				else
				{
					to_continue = 0;
				}
				to_continue = all_reduce(process_group(g), to_continue, boost::parallel::sum<int>());

				pspsssp.clear_allsource_sssp_prop_map(); //TODO: CLEAR AS WELL AS DELETE
				
				
				//pspsssp.printCentrality();
				
//				if(pid == 0)
//				{
//					//std::cout<<"***final***"<<std::endl;
//					{
//						typename std::set<vertex_size_t>::iterator itr = currSet.begin();
//						centrality_type c_value;
//						
//						int i = 0;
//						for(; itr != currSet.end(); ++itr) {
//							std::cout<<*itr<<":"<<get(cm, vertex(*itr,g))<<";";
//						}
//					}
//					std::cout<<std::endl;
//					std::cout<<g.processor()<<":Iterations:"<<iteration_counter<<std::endl;
//				}

			}
			
		//ask everyone to print locally
		pspsssp.divide_centrality_by_two();
		//pspsssp.printCentrality();
		
		
		synchronize(pg);
		
		if(pid == 0)
		{
			std::cout<<"***final***"<<std::endl;
			{
				typename std::set<vertex_size_t>::iterator itr = currSet.begin();
				centrality_type c_value;
				
				int i = 0;
				for(; itr != currSet.end(); ++itr) {
					std::cout<<*itr<<":"<<get(cm, vertex(*itr,g))<<std::endl;
				}
			}
			std::cout<<std::endl;
			std::cout<<g.processor()<<":num_sssps:"<<num_sssps<<std::endl;
		}
	}
	
}
#endif // PARALLEL_SAFE_PARTITIONED_SSSP_HPP
