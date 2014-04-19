/* 
 * File:   community-main.cpp
 * Author: charith
 *
 * Created on April 8, 2014, 9:48 AM
 */

#include <cstdlib>
#include <stdlib.h>
#include <math.h>
#include <string.h>
#include <stdio.h>
#include <iostream> 
#include <fstream>
#include <sstream>
#include <vector>
#include <algorithm>
#include <mpi.h>

#include "graph_binary.h"
#include "community.h"
using namespace std;

struct GraphData {
    unsigned int nb_nodes;
    unsigned long nb_links;
    double total_weight;

    vector<unsigned long> degrees;
    vector<unsigned int> links;
    vector<float> weights;
    vector<int> nodeToCom;

    vector<int> rSource;
    vector<int> rSink;
    vector<int> rPart;


};

char *filename = NULL;
char *remoteEdgesFile = NULL;
char *processor_name;
//char *remoteEdgesFile = NULL;
char *filename_part = NULL;
int type = UNWEIGHTED;
int nb_pass = 0;
double precision = 0.000001;
int display_level = -2;
int k1 = 16;

bool verbose = false;

vector< pair<int, int> > remoteMap(10, make_pair(-1, -1));

pair<vector<unsigned int>::iterator, vector<float>::iterator > neighbors(unsigned int node, GraphData data) {
    assert(node >= 0 && node < data.nb_nodes);

    if (node == 0)
        return make_pair(data.links.begin(), data.weights.begin());
    else if (data.weights.size() != 0)
        return make_pair(data.links.begin() + data.degrees[node - 1], data.weights.begin() + data.degrees[node - 1]);
    else
        return make_pair(data.links.begin() + data.degrees[node - 1], data.weights.begin());
}

std::vector<std::string> &split(const std::string &s, char delim, std::vector<std::string> &elems) {
    std::stringstream ss(s);
    std::string item;
    while (std::getline(ss, item, delim)) {
        elems.push_back(item);
    }
    return elems;
}

std::vector<std::string> split(const std::string &s, char delim) {
    std::vector<std::string> elems;
    split(s, delim, elems);
    return elems;
}

void
usage(char *prog_name, const char *more) {
    cerr << more;
    cerr << "usage: " << prog_name << " input_file [-w weight_file] [-p part_file] [-q epsilon] [-l display_level] [-v] [-h]" << endl << endl;
    cerr << "input_file: file containing the graph to decompose in communities." << endl;
    cerr << "-w file\tread the graph as a weighted one (weights are set to 1 otherwise)." << endl;
    cerr << "-p file\tstart the computation with a given partition instead of the trivial partition." << endl;
    cerr << "\tfile must contain lines \"node community\"." << endl;
    cerr << "-q eps\ta given pass stops when the modularity is increased by less than epsilon." << endl;
    cerr << "-l k\tdisplays the graph of level k rather than the hierachical structure." << endl;
    cerr << "\tif k=-1 then displays the hierarchical structure rather than the graph at a given level." << endl;
    cerr << "-v\tverbose mode: gives computation time, information about the hierarchy and modularity." << endl;
    cerr << "-h\tshow this usage message." << endl;
    exit(0);
}

void
parse_args(int argc, char **argv) {
    if (argc < 2)
        usage(argv[0], "Bad arguments number\n");

    for (int i = 1; i < argc; i++) {
        if (argv[i][0] == '-') {
            switch (argv[i][1]) {
                case 'r':
                    //type = WEIGHTED;
                    remoteEdgesFile = argv[i + 1];
                    i++;
                    break;
                case 'p':
                    filename_part = argv[i + 1];
                    i++;
                    break;
                case 'q':
                    precision = atof(argv[i + 1]);
                    i++;
                    break;
                case 'l':
                    display_level = atoi(argv[i + 1]);
                    i++;
                    break;
                case 'k':
                    k1 = atoi(argv[i + 1]);
                    i++;
                    break;
                case 'v':
                    verbose = true;
                    break;
                default:
                    usage(argv[0], "Unknown option\n");
            }
        } else {
            if (filename == NULL)
                filename = argv[i];
            else if (remoteEdgesFile == NULL)
                remoteEdgesFile = argv[i];
            else
                usage(argv[0], "More than one filename\n");
        }
    }
}

void
display_time(const char *str) {
    time_t rawtime;
    time(&rawtime);
    cerr << str << ": " << ctime(&rawtime);
}



/*[MPI_MAX_PROCESSOR_NAM
 * 
 */
int main(int argc, char** argv) {

    int rank, size,namelen;
    processor_name =new char[MPI_MAX_PROCESSOR_NAME];
    MPI_Init(&argc, &argv);    
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Get_processor_name(processor_name, &namelen);
    parse_args(argc, argv);

    stringstream rankS;
    rankS << rank;

    string s(filename);
    s += "_" + rankS.str() + ".bin";

    string r(remoteEdgesFile);
    r += "_" + rankS.str() + ".remote";

    vector<pair<int, int> > remoteMap;

    ifstream remoteFileStream(r.c_str());
    int remoteEdgeCount = 0;
    
    if(verbose)
        cerr << processor_name<< " : Remote File : " << r << " : graph file : " << s << endl;

    vector<int> rSource;
    vector<int> rSink;
    vector<int> rpart;


    for (string line; getline(remoteFileStream, line);) {
        vector<string> parts = split(line, ' ');
        string localV = parts[0];
        string mapping = parts[1];
        int source = atoi(localV.c_str());

        vector<string> mappingParts = split(mapping, ',');

        int sink = atoi(mappingParts[0].c_str());
        int partition = atoi(mappingParts[1].c_str());


        if (remoteMap.size() <= source) {

            remoteMap.resize(source + 1, make_pair(-1, -1));

        }

        remoteMap[source] = make_pair(sink, partition);

        rSource.push_back(source);
        rSink.push_back(sink);
        rpart.push_back(partition);

    }




    



    if (verbose)
        display_time("Begin");
    char* tmp = new char[s.length() + 1];
    strcpy(tmp, s.c_str());
    
   
    Community c(tmp, NULL, type, -1, precision);
    if (filename_part != NULL)
        c.init_partition(filename_part);
    Graph g;
    bool improvement = true;
    double mod = c.modularity(), new_mod;
    int level = 0;

    if (verbose) {
        cerr <<processor_name << ":"<< "level " << level << ":\n";
        display_time("  start computation");
        cerr << "  network size: "
                << c.g.nb_nodes << " nodes, "
                << c.g.nb_links << " links, "
                << c.g.total_weight << " weight." << endl;
    }

    improvement = c.one_level();
    
    
    if(verbose) {
        cerr << processor_name << " : one level done result = " << improvement << endl;
    }
    new_mod = c.modularity();
    if (++level == display_level)
        g.display();
    if (display_level == -1)
        c.display_partition();
    
    if(verbose) {
        cerr << processor_name << " creating new graph "<< endl;
    }
    g = c.partition2graph_binary();
    // c = Community(g, -1, precision);
    
    if(verbose) {
        cerr << processor_name << " Graph creation done "<< endl;
    }
    
    
    for (int i = 0; i < rSource.size(); i++) {
        rSource[i] = c.n2c_new[rSource[i]];
    }
    
    
    if (verbose)
        cerr << "  modularity increased from " << mod << " to " << new_mod << endl;

    mod = new_mod;
    if (verbose)
        display_time("  end computation from " + rank);
    if (verbose)
        cerr << "Start sending data" << endl;
    if (rank != 0) {
        MPI_Send(&g.nb_links, 1, MPI_LONG, 0, 1, MPI_COMM_WORLD);
        MPI_Send(&g.nb_nodes, 1, MPI_INT, 0, 1, MPI_COMM_WORLD);
        MPI_Send(&g.total_weight, 1, MPI_DOUBLE, 0, 1, MPI_COMM_WORLD);
        int nb_link = g.links.size();
        MPI_Send(&nb_link, 1, MPI_INT, 0, 1, MPI_COMM_WORLD);
        MPI_Send(&g.links.front(), g.links.size(), MPI_INT, 0, 1, MPI_COMM_WORLD);
        MPI_Send(&g.degrees.front(), g.degrees.size(), MPI_LONG, 0, 1, MPI_COMM_WORLD);
        int nb_weights = g.weights.size();
        MPI_Send(&nb_weights, 1, MPI_INT, 0, 1, MPI_COMM_WORLD);
        MPI_Send(&g.weights.front(), g.weights.size(), MPI_FLOAT, 0, 1, MPI_COMM_WORLD);

        int r_size = rSource.size();
        MPI_Send(&r_size, 1, MPI_INT, 0, 1, MPI_COMM_WORLD);
        MPI_Send(&rSource.front(), rSource.size(), MPI_INT, 0, 1, MPI_COMM_WORLD);
        MPI_Send(&rSink.front(), rSink.size(), MPI_INT, 0, 1, MPI_COMM_WORLD);
        MPI_Send(&rpart.front(), rpart.size(), MPI_INT, 0, 1, MPI_COMM_WORLD);

        MPI_Send(&c.n2c_new.front(), c.n2c_new.size(), MPI_INT, 0, 1, MPI_COMM_WORLD);
    }
    if (rank == 0) {


        GraphData data[size - 1];
        MPI_Status status;
        //get graph parts. 

        int total_nodes = 0;
        for (int i = 1; i < size; i++) {

            MPI_Recv(&data[i - 1].nb_links, 1, MPI_LONG, i, 1, MPI_COMM_WORLD, &status);
            MPI_Recv(&data[i - 1].nb_nodes, 1, MPI_INT, i, 1, MPI_COMM_WORLD, &status);
            MPI_Recv(&data[i - 1].total_weight, 1, MPI_DOUBLE, i, 1, MPI_COMM_WORLD, &status);
            int nb_links;
            MPI_Recv(&nb_links, 1, MPI_INT, i, 1, MPI_COMM_WORLD, &status);
            MPI_Recv(&data[i - 1].links.front(), nb_links, MPI_INT, i, 1,
                    MPI_COMM_WORLD, &status);
            MPI_Recv(&data[i - 1].degrees.front(), data[i - 1].nb_nodes, MPI_LONG, i, 1,
                    MPI_COMM_WORLD, &status);
            int nb_w;
            MPI_Recv(&nb_w, 1, MPI_INT, i, 1, MPI_COMM_WORLD, &status);
            MPI_Recv(&data[i - 1].weights.front(), nb_w, MPI_FLOAT, i, 1,
                    MPI_COMM_WORLD, &status);

            int r_size;
            MPI_Recv(&r_size, 1, MPI_INT, i, 1, MPI_COMM_WORLD, &status);
            MPI_Recv(&data[i - 1].rSource.front(), r_size, MPI_INT, i, 1,
                    MPI_COMM_WORLD, &status);
            MPI_Recv(&data[i - 1].rSink.front(), r_size, MPI_INT, i, 1,
                    MPI_COMM_WORLD, &status);
            MPI_Recv(&data[i - 1].rPart.front(), r_size, MPI_INT, i, 1,
                    MPI_COMM_WORLD, &status);
            MPI_Recv(&data[i - 1].nodeToCom.front(), data[i - 1].nb_nodes, MPI_INT, i, 1,
                    MPI_COMM_WORLD, &status);

            total_nodes += data[i - 1].nb_nodes;

        }





        //construct new graph
        Graph newG;
        newG.nb_nodes = total_nodes;
        newG.degrees.resize(total_nodes);
        newG.nb_links = 0;
        newG.total_weight = 0;
        double totalWeight = 0;
        int numberOfLinks = 0;
        for (int i = 0; i < size; i++) {

            if (i == 0) {
                for (int node = 0; node < g.nb_nodes; node++) {
                    //add local links
                    //add local weights
                    pair<vector<unsigned int>::iterator, vector<float>::iterator> p = g.neighbors(node);
                    int deg = g.nb_neighbors(node);

                    for (int e = 0; e < deg; e++) {
                        int neigh = *(p.first + e);
                        double weight = (g.weights.size() == 0) ? 1. : *(p.second + e);
                        newG.total_weight += weight;

                        newG.links.push_back(neigh);
                        newG.weights.push_back(weight);

                    }


                    //add remote links
                    //add remote weights   

                    map<int, float> m;
                    map<int, float>::iterator it;

                    for (int ri = 0; ri < rSource.size(); ri++) {
                        if (rSource[ri] == node) {
                            int targetPart = rpart[ri];
                            int targetComm = data[targetPart].nodeToCom[rSink[ri]];
                            int gap = 0;
                            for (int k = 1; k < targetPart; k++) {
                                gap += data[k].nb_nodes;
                            }
                            targetComm += gap;
                            it = m.find(targetComm);

                            if (it == m.end()) {
                                m.insert(make_pair(targetComm, 1.0f));
                            } else {
                                it->second += 1.0f;
                            }
                        }

                    }

                    int num_rEdges = m.size();
                    for (it = m.begin(); it != m.end(); it++) {
                        totalWeight += it->second;
                        newG.links.push_back(it->first);
                        newG.weights.push_back(it->second);
                    }

                    //update degrees and number of links;
                    newG.nb_links += num_rEdges + deg;
                    newG.degrees[node] = (node == 0) ? (num_rEdges + deg) : newG.degrees[node - 1]+(num_rEdges + deg);

                }

            } else {

                for (int node = 0; node < data[i].nb_nodes; node++) {
                    //add local links
                    //add local weights
                    pair<vector<unsigned int>::iterator, vector<float>::iterator> p = neighbors(node, data[i]);
                    int deg = node == 0 ? data[i].degrees[0] : (data[i].degrees[node] - data[i].degrees[node - 1]);

                    for (int e = 0; e < deg; e++) {
                        int neigh = *(p.first + e);
                        double weight = (data[i].weights.size() == 0) ? 1. : *(p.second + e);
                        newG.total_weight += weight;

                        newG.links.push_back(neigh);
                        newG.weights.push_back(weight);

                    }


                    //add remote links
                    //add remote weights   

                    map<int, float> m;
                    map<int, float>::iterator it;

                    for (int ri = 0; ri < data[i].rSource.size(); ri++) {
                        if (data[i].rSource[ri] == node) {
                            int targetPart = data[i].rPart[ri];
                            int targetComm = data[targetPart].nodeToCom[data[i].rSink[ri]];
                            int gap = 0;
                            for (int k = 1; k < targetPart; k++) {
                                gap += data[k].nb_nodes;
                            }
                            targetComm += gap;
                            it = m.find(targetComm);

                            if (it == m.end()) {
                                m.insert(make_pair(targetComm, 1.0f));
                            } else {
                                it->second += 1.0f;
                            }
                        }

                    }

                    int num_rEdges = m.size();
                    for (it = m.begin(); it != m.end(); it++) {
                        totalWeight += it->second;
                        newG.links.push_back(it->first);
                        newG.weights.push_back(it->second);
                    }

                    //update degrees and number of links;
                    newG.nb_links += num_rEdges + deg;
                    newG.degrees[node] = (node == 0) ? (num_rEdges + deg) : newG.degrees[node - 1]+(num_rEdges + deg);

                }

            }


        }


        //Now graph construction is done

        c = Community(newG, -1, precision);

        mod = c.modularity();
        if (verbose)
            cerr << " new modularity " << mod << endl;

        improvement = true;

        do {

            if (verbose) {
                cerr << "level " << level << ":\n";
                display_time("  start computation");
                cerr << "  network size: "
                        << c.g.nb_nodes << " nodes, "
                        << c.g.nb_links << " links, "
                        << c.g.total_weight << " weight." << endl;
            }

            improvement = c.one_level();
            new_mod = c.modularity();
            if (++level == display_level)
                newG.display();
            if (display_level == -1)
                c.display_partition();
            newG = c.partition2graph_binary();
            c = Community(newG, -1, precision);

            if (verbose)
                cerr << "  modularity increased from " << mod << " to " << new_mod << endl;

            mod = new_mod;
            if (verbose)
                display_time("end computation");

            if (filename_part != NULL && level == 1) // do at least one more computation if partition is provided
                improvement = true;

        } while (improvement);

    }

    MPI_Finalize();

    return 0;
}

