// File: graph_binary.cpp
// -- graph handling source
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

#include <sys/mman.h>
#include <fstream>
#include "graph_binary_better.h"
#include "math.h"       

GraphB::GraphB() {
    degrees = new BetterVector<unsigned long>();
    links = new BetterVector<unsigned int>();
    weights = new BetterVector<float>();
}

void
GraphB::display() {

    for (unsigned int node = 0; node < nb_nodes; node++) {
        pair<vector<unsigned int>::iterator, vector<float>::iterator > p = neighbors(node);
        cout << node << ":";
        for (unsigned int i = 0; i < nb_neighbors(node); i++) {
            if (true) {
                if (weights->size != 0)
                    cout << " (" << *(p.first + i) << " " << *(p.second + i) << ")";
                else
                    cout << " " << *(p.first + i);
            }
        }
        
        p = remote_neighbors(node);
        for (unsigned int i = 0; i < nb_remote_neighbors(node); i++) {
            if (true) {
                if (remote_weights.find(node)->second.size() != 0)
                    cout << " (" << *(p.first + i) << " " << *(p.second + i) << ")";
                else
                    cout << " " << *(p.first + i);
            }
        }         
                 
        cout << endl;
    }
}

void
GraphB::display_reverse() {
    for (unsigned int node = 0; node < nb_nodes; node++) {
        pair<vector<unsigned int>::iterator, vector<float>::iterator > p = neighbors(node);
        for (unsigned int i = 0; i < nb_neighbors(node); i++) {
            if (node>*(p.first + i)) {
                if (weights->size != 0)
                    cout << *(p.first + i) << " " << node << " " << *(p.second + i) << endl;
                else
                    cout << *(p.first + i) << " " << node << endl;
            }
        }
    }
}

bool
GraphB::check_symmetry() {
    int error = 0;
    for (unsigned int node = 0; node < nb_nodes; node++) {
        pair<vector<unsigned int>::iterator, vector<float>::iterator > p = neighbors(node);
        for (unsigned int i = 0; i < nb_neighbors(node); i++) {
            unsigned int neigh = *(p.first + i);
            float weight = *(p.second + i);

            pair<vector<unsigned int>::iterator, vector<float>::iterator > p_neigh = neighbors(neigh);
            for (unsigned int j = 0; j < nb_neighbors(neigh); j++) {
                unsigned int neigh_neigh = *(p_neigh.first + j);
                float neigh_weight = *(p_neigh.second + j);

                if (node == neigh_neigh && weight != neigh_weight) {
                    cout << node << " " << neigh << " " << weight << " " << neigh_weight << endl;
                    if (error++ == 10)
                        exit(0);
                }
            }
        }
    }
    return (error == 0);
}

void
GraphB::display_binary(char *outfile) {
    cout << "Display binary not supported" <<endl;
}
