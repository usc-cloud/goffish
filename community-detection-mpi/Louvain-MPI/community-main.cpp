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
#include "graph_binary_better.h"

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

pair<vector<unsigned int>::iterator, vector<float>::iterator > neighbors(unsigned int node, GraphData & data) {
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

    time_t time_begin, time_end;
    time(&time_begin);

    int rank, size, namelen;
    //processor_name =new char[MPI_MAX_PROCESSOR_NAME];
    MPI_Init(&argc, &argv);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

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


    vector<int> rSource;
    vector<int> rSink;
    vector<int> rpart;
    MPI_Status status;


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
        cerr << rank << ":" << "level " << level << ":\n";
        display_time("  start computation");
        cerr << "  network size: "
                << c.g.nb_nodes << " nodes, "
                << c.g.nb_links << " links, "
                << c.g.total_weight << " weight." << endl;
    }



    improvement = c.one_level();
    new_mod = c.modularity();



    if (++level == display_level)
        g.display();
    if (display_level == -1)
        c.display_partition();

    //    if(verbose) {
    //        cerr << processor_name << " creating new graph "<< endl;
    //    }
    g = c.partition2graph_binary();


    if (verbose) {
        cerr << rank << ":" << "level " << level << ":\n";
        display_time("  start computation");
        cerr << "  network size: "
                << g.nb_nodes << " nodes, "
                << g.nb_links << " links, "
                << g.total_weight << " weight." << endl;
    }




    for (int i = 0; i < rSource.size(); i++) {
        rSource[i] = c.n2c_new[rSource[i]];
    }


    int *level_one_n_nodes = new int[size];
    unsigned long* level_one_final_degree = new unsigned long[size];

    level_one_n_nodes[rank] = g.nb_nodes;
    level_one_final_degree[rank] = g.degrees[g.degrees.size() - 1];

    for (int i = 0; i < size; i++) {

        if (i != rank) {
            MPI_Send(&g.nb_nodes, 1, MPI_INT, i, 1, MPI_COMM_WORLD);
            MPI_Send(&g.degrees[g.degrees.size() - 1], 1, MPI_UNSIGNED_LONG, i, 2, MPI_COMM_WORLD);
        }

    }


    for (int i = 0; i < size; i++) {

        if (i != rank) {
            int v;
            unsigned long d;
            MPI_Recv(&v, 1, MPI_INT, i, 1, MPI_COMM_WORLD, &status);
            MPI_Recv(&d, 1, MPI_UNSIGNED_LONG, i, 2, MPI_COMM_WORLD, &status);
            level_one_n_nodes[i] = v;
            level_one_final_degree[i] = d;
           
        }

    }

    //Re-Number in parallel


    int gap = 0;

    int *gaps = new int[size];


    for (int i = 0; i < size; i++) {

        gaps[i] = gap;
        if (i != rank)
            gap += level_one_n_nodes[i];
        else
            gap += g.nb_nodes;

    }


    for (int i = 0; i < g.links.size(); i++) {
        g.links[i] += gaps[rank];
    }


    for (int i = 0; i < rSource.size(); i++) {
        rSource[i] += gaps[rank];
    }

    //renumber n2c_new;
    for (int i = 0; i < c.n2c_new.size(); i++) {

        c.n2c_new[i] += gaps[rank];

    }
    
    
    

    //update degree in parallel
    unsigned long deg_gap = 0;
     
    for (int i = 0; i < rank; i++) {

        deg_gap += level_one_final_degree[i];
      

    }
    
    //   cerr <<"Rank:" << rank << " Gap:"<<deg_gap <<endl;
    
    if (rank != 0)
        for (int i = 0; i < g.degrees.size(); i++) {
            g.degrees[i] += deg_gap;
        }




    if (verbose)
        cerr << "  modularity increased from " << mod << " to " << new_mod << endl;

    mod = new_mod;
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

        //cout << "Remote date sizes source , sink , part " << rSource.size() << "," << rSink.size() << "," << rpart.size() << endl;
        MPI_Send(&r_size, 1, MPI_INT, 0, 1, MPI_COMM_WORLD);
        MPI_Send(&rSource.front(), rSource.size(), MPI_INT, 0, 1, MPI_COMM_WORLD);
        MPI_Send(&rSink.front(), rSink.size(), MPI_INT, 0, 1, MPI_COMM_WORLD);
        MPI_Send(&rpart.front(), rpart.size(), MPI_INT, 0, 1, MPI_COMM_WORLD);
        int nc_size = c.n2c_new.size();
        MPI_Send(&nc_size, 1, MPI_INT, 0, 1, MPI_COMM_WORLD);
        MPI_Send(&c.n2c_new.front(), c.n2c_new.size(), MPI_INT, 0, 1, MPI_COMM_WORLD);



    }
    if (rank == 0) {


        GraphData data[size - 1];
        //get graph parts. 

        int total_nodes = 0;
        for (int i = 1; i < size; i++) {

            MPI_Recv(&data[i - 1].nb_links, 1, MPI_LONG, i, 1, MPI_COMM_WORLD, &status);
            MPI_Recv(&data[i - 1].nb_nodes, 1, MPI_INT, i, 1, MPI_COMM_WORLD, &status);
            MPI_Recv(&data[i - 1].total_weight, 1, MPI_DOUBLE, i, 1, MPI_COMM_WORLD, &status);

            int nb_links;
            MPI_Recv(&nb_links, 1, MPI_INT, i, 1, MPI_COMM_WORLD, &status);

            data[i - 1].links.resize(nb_links);
            MPI_Recv(&data[i - 1].links.front(), nb_links, MPI_INT, i, 1,
                    MPI_COMM_WORLD, &status);


            data[i - 1].degrees.resize(data[i - 1].nb_nodes);
            MPI_Recv(&data[i - 1].degrees.front(), data[i - 1].nb_nodes, MPI_LONG, i, 1,
                    MPI_COMM_WORLD, &status);

            int nb_w;
            MPI_Recv(&nb_w, 1, MPI_INT, i, 1, MPI_COMM_WORLD, &status);
            data[i - 1].weights.resize(nb_w);
            MPI_Recv(&data[i - 1].weights.front(), nb_w, MPI_FLOAT, i, 1,
                    MPI_COMM_WORLD, &status);

            int r_size;
            MPI_Recv(&r_size, 1, MPI_INT, i, 1, MPI_COMM_WORLD, &status);
            data[i - 1].rSource.resize(r_size);
            MPI_Recv(&data[i - 1].rSource.front(), r_size, MPI_INT, i, 1,
                    MPI_COMM_WORLD, &status);
            data[i - 1].rSink.resize(r_size);
            MPI_Recv(&data[i - 1].rSink.front(), r_size, MPI_INT, i, 1,
                    MPI_COMM_WORLD, &status);
            data[i - 1].rPart.resize(r_size);
            MPI_Recv(&data[i - 1].rPart.front(), r_size, MPI_INT, i, 1,
                    MPI_COMM_WORLD, &status);
            int nc_size;
            MPI_Recv(&nc_size, 1, MPI_INT, i, 1, MPI_COMM_WORLD, &status);

            data[i - 1].nodeToCom.resize(nc_size);
            MPI_Recv(&data[i - 1].nodeToCom.front(), nc_size, MPI_INT, i, 1,
                    MPI_COMM_WORLD, &status);
            total_nodes += data[i - 1].nb_nodes;

        }


      

        //construct new graph
        GraphB newG = GraphB();

        newG.nb_nodes = g.nb_nodes;
        //s newG->degrees.resize(g.nb_nodes);
        newG.nb_links = 0;
        newG.total_weight = 0;


        // merge graphs;






        for (int i = 0; i < size; i++) {


            if (i == 0) {
                //local graph

                newG.degrees->extend(g.degrees);
                newG.links->extend(g.links);
                newG.weights->extend(g.weights);
                newG.nb_links += g.nb_links;
                newG.total_weight += g.total_weight;

            } else {


                

                newG.degrees->extend(data[i - 1].degrees);
                newG.links->extend(data[i - 1].links);
                newG.weights->extend(data[i - 1].weights);
                newG.nb_links += data[i - 1].nb_links;
                newG.total_weight += data[i - 1].total_weight;
                newG.nb_nodes += data[i - 1].nb_nodes;
            }


        }






        map<int, vector<unsigned int> > re;
        map<int, vector<float> > weight;
        for (int i = 0; i < size; i++) {


            if (i == 0) {

                map<pair<int, unsigned int>, float> m;
                map<pair<int, unsigned int>, float>::iterator it;
                for (int j = 0; j < rSource.size(); j++) {

                    int sink = rSink[j];
                    int sinkPart = rpart[j];

                    int target = data[sinkPart - 1].nodeToCom[sink];

                    it = m.find(make_pair(rSource[j], target));

                    if (it == m.end()) {
                        m.insert(make_pair(make_pair(rSource[j], target), 1.0f));
                    } else {
                        it->second += 1.0f;
                    }

                }

                newG.nb_links += m.size();
                //newG.total_weight += m.size();

                map<int, vector<unsigned int> >::iterator remoteEdgeIt;
                map<int, vector<float> >::iterator remoteWIt;


                for (it = m.begin(); it != m.end(); it++) {
                    pair<int, unsigned int> e = it->first;
                    float w = it->second;

                    remoteEdgeIt = re.find(e.first);
                    remoteWIt = weight.find(e.first);

                    if (remoteEdgeIt == re.end()) {

                        vector<unsigned int> list;
                        list.push_back(e.second);
                        re.insert(make_pair(e.first, list));

                        vector<float> wList;
                        wList.push_back(w);
                        weight.insert(make_pair(e.first, wList));

                    } else {
                        remoteEdgeIt->second.push_back(e.second);
                        if (remoteWIt != weight.end()) {
                            remoteWIt->second.push_back(w);
                        }
                    }

                }


            } else {
                map<pair<int, unsigned int>, float> m;
                map<pair<int, unsigned int>, float>::iterator it;


                for (int j = 0; j < data[i - 1].rSource.size(); j++) {

                    int sink = data[i - 1].rSink[j];
                    int sinkPart = data[i - 1].rPart[j];

                    int target;

                    if (sinkPart == 0) {
                        target = c.n2c_new[sink];
                    } else {
                        target = data[sinkPart - 1].nodeToCom[sink];
                    }
                    it = m.find(make_pair(data[i - 1].rSource[j], target));

                    if (it == m.end()) {
                        m.insert(make_pair(make_pair(data[i - 1].rSource[j], target), 1.0f));
                    } else {
                        it->second += 1.0f;
                    }

                }


                newG.nb_links += m.size();
                //newG.total_weight += m.size();

                map<int, vector<unsigned int> >::iterator remoteEdgeIt;
                map<int, vector<float> >::iterator remoteWIt;

                for (it = m.begin(); it != m.end(); it++) {
                    pair<int, int> e = it->first;
                    float w = it->second;

                    remoteEdgeIt = re.find(e.first);
                    remoteWIt = weight.find(e.first);

                    if (remoteEdgeIt == re.end()) {

                        vector<unsigned int> list;
                        list.push_back(e.second);
                        re.insert(make_pair(e.first, list));

                        vector<float> wList;
                        wList.push_back(w);
                        weight.insert(make_pair(e.first, wList));

                    } else {
                        remoteEdgeIt->second.push_back(e.second);
                        if (remoteWIt != weight.end()) {
                            remoteWIt->second.push_back(w);
                        }
                    }

                }


            }
        }


        //update the graph

        newG.add_remote_edges(re, weight);




        c = Community(newG, -1, precision);
        mod = c.modularity_new();
        if (verbose)
            cerr << " new modularity " << mod << endl;

        improvement = true;

        if (verbose) {
            cerr << "level " << level << ":\n";
            display_time("  start computation");
            cerr << "  network size: "
                    << c.gB.nb_nodes << " nodes, "
                    << c.gB.nb_links << " links, "
                    << c.gB.total_weight << " weight." << endl;
        }

        improvement = c.one_level_new();
        new_mod = c.modularity_new();
        if (++level == display_level)
            newG.display();
        if (display_level == -1)
            c.display_partition();
        Graph g2 = c.partition2graph_binary_new();
        c = Community(g2, -1, precision);
        newG.cleanup();

        if (verbose)
            cerr << "  modularity increased from " << mod << " to " << new_mod << endl;

        mod = new_mod;
        if (verbose)
            display_time("end computation");

        if (filename_part != NULL && level == 1) // do at least one more computation if partition is provided
            improvement = true;


        do {

            if (verbose) {
                cerr << "level in loop " << level << ":\n";
                display_time("  start computation");
                cerr << "  network size: "
                        << c.g.nb_nodes << " nodes, "
                        << c.g.nb_links << " links, "
                        << c.g.total_weight << " weight." << endl;
            }

            improvement = c.one_level();

            new_mod = c.modularity();
            if (++level == display_level)
                g2.display();
            if (display_level == -1)
                c.display_partition();
            g2 = c.partition2graph_binary();
            c = Community(g2, -1, precision);

            if (verbose)
                cerr << "  modularity increased from " << mod << " to " << new_mod << endl;

            mod = new_mod;
            if (verbose)
                display_time("end computation");

            if (filename_part != NULL && level == 1) // do at least one more computation if partition is provided
                improvement = true;

        } while (improvement);

        cerr << new_mod << endl;

    }

    MPI_Finalize();

    time(&time_end);
    if (verbose) {
        display_time("End");
        cerr << "Total duration: " << (time_end - time_begin) << " sec." << endl;
    }

    return 0;
}

