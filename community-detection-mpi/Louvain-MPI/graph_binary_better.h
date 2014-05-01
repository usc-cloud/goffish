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

#ifndef GRAPHB_H
#define GRAPHB_H

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
#include "BetterVector.h"

#define WEIGHTED   0
#define UNWEIGHTED 1

using namespace std;

class GraphB {
public:
    unsigned int nb_nodes;
    unsigned long nb_links;
    double total_weight;

    BetterVector<unsigned long> *degrees;
    BetterVector<unsigned int> *links;
    BetterVector<float> *weights;


    map<int, vector<unsigned int> > remoteEdges;
    map<int, vector<float> > remote_weights;

    GraphB();


    GraphB(int nb_nodes, int nb_links, double total_weight, int *degrees, int *links, float *weights);

    void display(void);
    void display_reverse(void);
    void display_binary(char *outfile);
    bool check_symmetry();


    // return the number of neighbors (degree) of the node
    inline unsigned int nb_neighbors(unsigned int node);

    inline unsigned int nb_remote_neighbors(unsigned int node);


    // return the number of self loops of the node
    inline double nb_selfloops(unsigned int node);

    // return the weighted degree of the node
    inline double weighted_degree(unsigned int node);

    inline double weighted_degree_wremote(unsigned int node);

    // return pointers to the first neighbor and first weight of the node
    inline pair<vector<unsigned int>::iterator, vector<float>::iterator > neighbors(unsigned int node);

    inline pair<vector<unsigned int>::iterator, vector<float>::iterator > remote_neighbors(unsigned int node);

    inline void add_remote_edges(map<int, vector<unsigned int> > re, map<int, vector<float> >);

    inline void cleanup();
};

inline unsigned int
GraphB::nb_neighbors(unsigned int node) {
    assert(node >= 0 && node < nb_nodes);

    if (node == 0)
        return degrees->get(0);
    else
        return degrees->get(node) - degrees->get(node - 1);
}

inline void
GraphB::add_remote_edges(map<int, vector<unsigned int> > re, map<int, vector<float> > w) {


    remoteEdges = re;
    remote_weights = w;

    map<int, vector<float> >::iterator it = w.begin();

    double res = 0;
    for (; it != w.end(); it++) {
        int node = it->first;
        vector<float> ws = it->second;

        if (ws.size() == 0) {
            int rdeg = nb_remote_neighbors(node);
            res +=rdeg;
        } else
            for (int i = 0; i < ws.size(); i++) {
                res += ws[i];
            }
    }


    total_weight += res;


    map<int, vector<unsigned int> >::iterator itr = re.begin();

    int nlr = 0;
    for (; itr != re.end(); it++) {
        int node = itr->first;
        vector<unsigned int> res = itr->second;

        nlr += res.size();

    }

    nb_links += nlr;

}

inline unsigned int GraphB::nb_remote_neighbors(unsigned int node) {
    assert(node >= 0 && node < nb_nodes);
    return remoteEdges[node].size();
}

inline void GraphB::cleanup() {
    delete degrees;
    delete links;
    delete weights;
}

inline double
GraphB::nb_selfloops(unsigned int node) {
    assert(node >= 0 && node < nb_nodes);

    pair<vector<unsigned int>::iterator, vector<float>::iterator > p = neighbors(node);
    for (unsigned int i = 0; i < nb_neighbors(node); i++) {
        if (*(p.first + i) == node) {
            if (weights->size != 0)
                return (double) *(p.second + i);
            else
                return 1.;
        }
    }
    return 0.;
}

inline double
GraphB::weighted_degree(unsigned int node) {
    assert(node >= 0 && node < nb_nodes);

    if (weights->size == 0)
        return (double) nb_neighbors(node);
    else {
        pair<vector<unsigned int>::iterator, vector<float>::iterator > p = neighbors(node);
        double res = 0;
        for (unsigned int i = 0; i < nb_neighbors(node); i++) {
            res += (double) *(p.second + i);
        }
        return res;
    }
}

inline double
GraphB::weighted_degree_wremote(unsigned int node) {
    assert(node >= 0 && node < nb_nodes);
    if (remote_weights[node].size() == 0) {
        return nb_remote_neighbors(node);
    } else {
        pair<vector<unsigned int>::iterator, vector<float>::iterator > p = remote_neighbors(node);
        int deg = nb_remote_neighbors(node);
        double res = 0;

        for (unsigned int i = 0; i < deg; i++) {
            res += (double) *(p.second + 1);
        }

        return res;

    }

}

inline pair<vector<unsigned int>::iterator, vector<float>::iterator >
GraphB::neighbors(unsigned int node) {
    assert(node >= 0 && node < nb_nodes);

    if (node == 0)
        return make_pair(links->getPointer(0), weights->getPointer(0));
    else if (weights->size != 0)
        return make_pair(links->getPointer(degrees->get(node - 1)), weights->getPointer(degrees->get(node - 1)));
    else
        return make_pair(links->getPointer(degrees->get(node - 1)), weights->getPointer(0));
}

inline pair<vector<unsigned int>::iterator, vector<float>::iterator >
GraphB::remote_neighbors(unsigned int node) {
    assert(node >= 0 && node < nb_nodes);
  //  vector<unsigned int>::iterator
    return make_pair(remoteEdges.find(node)->second.begin(), remote_weights.find(node)->second.begin());
}


#endif 
