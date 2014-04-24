// File: graph_binary.h
// -- graph handling header file
//-----------------------------------------------------------------------------
// Community detection 
// Based on the article "Fast unfolding of community hierarchies in large networks"
// Copyright (C) 2008 V. Blondel, J.-L. Guillaume, R. Lambiotte, E. Lefebvre
//
// This program must not be distributed without agreement of the above mentionned authors.
//-----------------------------------------------------------------------------
// Author   : E. Lefebvre, adapted by J.-L. Guillaume
// Email    : jean-loup.guillaume@lip6.fr
// Location : Paris, France
// Time	    : February 2008
//-----------------------------------------------------------------------------
// see readme.txt for more details

#ifndef GRAPH_H
#define GRAPH_H

#include <stdlib.h>
#include <stdio.h>
#include <assert.h>
#include <malloc.h>
#include <iostream>
#include <iomanip>
#include <fstream>
#include <vector>
#include <map>
#include <algorithm>

#define WEIGHTED   0
#define UNWEIGHTED 1

using namespace std;

class GraphB {
 public:
  unsigned int nb_nodes;
  unsigned long nb_links;
  double total_weight;  

  vector<unsigned long> degrees;
  vector<unsigned int> links;
  vector<float> weights;

  GraphB();

  
  GraphB(int nb_nodes, int nb_links, double total_weight, int *degrees, int *links, float *weights);

  void display(void);
  void display_reverse(void);
  void display_binary(char *outfile);
  bool check_symmetry();


  // return the number of neighbors (degree) of the node
  inline unsigned int nb_neighbors(unsigned int node);

  // return the number of self loops of the node
  inline double nb_selfloops(unsigned int node);

  // return the weighted degree of the node
  inline double weighted_degree(unsigned int node);

  // return pointers to the first neighbor and first weight of the node
  inline pair<vector<unsigned int>::iterator, vector<float>::iterator > neighbors(unsigned int node);
};


inline unsigned int
GraphB::nb_neighbors(unsigned int node) {
  assert(node>=0 && node<nb_nodes);

  if (node==0)
    return degrees[0];
  else
    return degrees[node]-degrees[node-1];
}

inline double
GraphB::nb_selfloops(unsigned int node) {
  assert(node>=0 && node<nb_nodes);

  pair<vector<unsigned int>::iterator, vector<float>::iterator > p = neighbors(node);
  for (unsigned int i=0 ; i<nb_neighbors(node) ; i++) {
    if (*(p.first+i)==node) {
      if (weights.size()!=0)
	return (double)*(p.second+i);
      else 
	return 1.;
    }
  }
  return 0.;
}

inline double
GraphB::weighted_degree(unsigned int node) {
  assert(node>=0 && node<nb_nodes);

  if (weights.size()==0)
    return (double)nb_neighbors(node);
  else {
    pair<vector<unsigned int>::iterator, vector<float>::iterator > p = neighbors(node);
    double res = 0;
    for (unsigned int i=0 ; i<nb_neighbors(node) ; i++) {
      res += (double)*(p.second+i);
    }
    return res;
  }
}

inline pair<vector<unsigned int>::iterator, vector<float>::iterator >
GraphB::neighbors(unsigned int node) {
  assert(node>=0 && node<nb_nodes);

  if (node==0)
    return make_pair(links.begin(), weights.begin());
  else if (weights.size()!=0)
    return make_pair(links.begin()+degrees[node-1], weights.begin()+degrees[node-1]);
  else
    return make_pair(links.begin()+degrees[node-1], weights.begin());
}


#endif // GRAPH_H
