/**
 * Some of the code was copied from https://sites.google.com/site/findcommunities/
 */

#include "graph.h"
#include <string.h>
#include <sstream>

using namespace std;

char *infile = NULL;
char *outfile = NULL;
char *partition_file = NULL;
int numberOfPartitions = 1;


void printPartition(vector<pair<int,int> > edgeList , int p);

void
usage(char *prog_name, const char *more) {
    cerr << more;
    cerr << "usage: " << prog_name << " -i input_file -o outfile -p partition file -n number of partitions" << endl << endl;
    exit(0);
}

void
parse_args(int argc, char **argv) {
    for (int i = 1; i < argc; i++) {
        if (argv[i][0] == '-') {
            switch (argv[i][1]) {
                case 'i':
                    if (i == argc - 1)
                        usage(argv[0], "Infile missing\n");
                    infile = argv[i + 1];
                    i++;
                    break;
                case 'o':
                    if (i == argc - 1)
                        usage(argv[0], "Outfile missing\n");
                    outfile = argv[i + 1];
                    i++;
                    break;
                case 'p':
                    if (i == argc - 1)
                        usage(argv[0], "partition missing\n");
                    partition_file = argv[i + 1];
                    i++;
                    break;
                case 'n':
                    if (i == argc - 1)
                        usage(argv[0], "number of partitions missing\n");
                    numberOfPartitions = atoi(argv[i + 1]);
                    i++;
                    break;
                default:
                    usage(argv[0], "Unknown option\n");
            }
        } else {
            usage(argv[0], "More than one filename\n");
        }
    }
    if (infile == NULL || outfile == NULL)
        usage(argv[0], "In or outfile missing\n");
}

/*
 *This will accept original graph file, partition file and number of partitions.
 *This will partition the graph in to multiple bin files based on number of partitions.
 * Each file will will have its local numbering 
 * There is a remote file which contain the remote edges. 
 * This will have the remote edges and associated partition number
 */
int main(int argc, char** argv) {
    parse_args(argc, argv);

    //  infile = "~/metis_test/test_louvan.graph";
    //  outfile = "~/metis_test/test_part";
    //  partition_file = "~/metis_test/test.graph.part.2";
    //  numberOfPartitions = 2;

    cout << "Intput file: " << infile << " Output " << outfile << " Partitions : " << partition_file << " number of Part : " << numberOfPartitions << endl;
    ifstream finput;
    finput.open(infile, fstream::in);

    vector<pair<int, int> > edgeList;
    int nb_links = 0;

    while (!finput.eof()) {
        unsigned int src, dest;
        finput >> src >> dest;
        if (finput.eof())
            break;
        edgeList.push_back(make_pair(src, dest));
        nb_links++;
        cout << src << " " << dest << endl;
        if (finput.eof())
            break;

    }
    finput.close();

    cout << "Loading graph done" << edgeList.size() << endl;

    vector<int> partitionMap;
    ifstream fpartition;
    fpartition.open(partition_file, fstream::in);

    int vid = 0;
    //string line;
    while (!fpartition.eof()) {
        int partition;
        fpartition >> partition;
        if (fpartition.eof())
            break;
        partitionMap.push_back(partition);
        cout << vid << ":" << partitionMap[vid] << endl;

        vid++;
    }

    cout << "Loading Partition done size : " << vid << endl;


    //   ofstream *fpartitions = new ofstream[numberOfPartitions];
    //   ofstream *fremoteMap = new ofstream[numberOfPartitions];
    //   
    //   for(int i =0 ; i< numberOfPartitions; i++) {
    //       fpartitions[i] = new ofstream;
    //       fremoteMap[i] = new ofstream;
    //       char* name = outfile + "_" + i + ".bin";
    //       char* remoteName = outfile + "_" + i + ".remote";
    //       fpartitions[i].open(name,fstream::out|fstream::binary);
    //       fremoteMap[i].open(name,fstream::out);      
    //   }
    //   



    vector<vector<pair<int, int> > > partitions;
    partitions.resize(numberOfPartitions);
    vector<vector<pair<int, int> > > remoteEdges;
    remoteEdges.resize(numberOfPartitions);


    for (int i = 0; i < edgeList.size(); i++) {

        int source = edgeList[i].first;
        int sink = edgeList[i].second;

        if ((source > partitionMap.size()) || (sink > partitionMap.size())) {
            cout << " ERROR : " << sink << " : " << source << " " << endl;


        }

        if (partitionMap[source - 1] == partitionMap[sink - 1]) {
            //in same partition
            cout << "Edge : " << edgeList[i].first << ","
                    << edgeList[i].second << ":" << partitionMap[source - 1] << endl;


            partitions[partitionMap[source - 1]].push_back(edgeList[i]);
        } else {
            //in different partitions

            cout << "REdge : " << edgeList[i].first << ","
                    << edgeList[i].second << ":" << partitionMap[source - 1] << endl;
            remoteEdges[partitionMap[source - 1]].push_back(edgeList[i]);
        }

    }



    cout << "Partitioning done" << endl;
    vector<pair<int, int> > oldToNewMap;
    oldToNewMap.resize(vid);

    for (int i = 0; i < numberOfPartitions; i++) {
        vector<int> map;
        map.resize(vid);
        int v = 1;
        for (int j = 0; j < partitions[i].size(); j++) {

            int source = partitions[i][j].first;
            int sink = partitions[i][j].second;
          
            if (source == sink) {
                sink = -1;
            }
          
            if (map[source] != 0) {
                source = map[source];
            } else {

                map[source] = v;
                oldToNewMap[source-1] = make_pair(v, i);
              //  cout << vid << "in " << i << endl;
                source = v++;
            }

            if (sink == -1) {
                partitions[i][j].first = source;
                partitions[i][j].second = source;
                continue;
            }

            if (map[sink] != 0) {
                sink = map[sink];
            } else {
                map[sink] = v;
                oldToNewMap[sink-1] = make_pair(v, i);
            //    cout << vid << "in " << i << endl;
                sink = v++;
            }
            partitions[i][j].first = source;
            partitions[i][j].second = sink;

        }
    }


    cout << "Mapping done" << endl;

    for (int i = 0; i < numberOfPartitions; i++) {
        ofstream fremoteList;
        string s(outfile);
        stringstream ss;
        ss << i;
        string remoteName = s + "_" + ss.str() + ".remote";
        char *cstr = new char[remoteName.length() + 1];
        strcpy(cstr, remoteName.c_str());
        fremoteList.open(cstr, fstream::out);
        delete [] cstr;
        for (int j = 0; j < remoteEdges[i].size(); j++) {
            int source = remoteEdges[i][j].first;
            int sink = remoteEdges[i][j].second;
            fremoteList << (oldToNewMap[source-1].first -1) << " " << (oldToNewMap[sink-1].first - 1) << "," << ( oldToNewMap[sink-1].second - 1)<< endl;
        }

        fremoteList.close();

        cout << "Partition " << i << " remote done" << endl;

      //  for (int j = 0; j < partitions[i].size(); j++) {
            //printPartition(partitions[i],i);
            Graph g(partitions[i]);
            g.clean(UNWEIGHTED);
            string st(outfile);
            stringstream sst;
            sst << i;
            string sout = st + "_" + sst.str() + ".bin";
            cstr = new char[sout.length() + 1];
            strcpy(cstr, sout.c_str());
                
            cout << "********* Partition " << i << " ********************" <<endl;
            g.display(UNWEIGHTED);
            g.display_binary(cstr, NULL, UNWEIGHTED);
            delete [] cstr;
      //  }

        cout << "Partition " << i << " local done" << endl;

    }


    
   



    return 0;
}

 void printPartition(vector<pair<int,int> > edgeList , int p) {
        int size = edgeList.size();
        
        cout << "*************Partition " << p << "**************" <<endl;
         
        for(int i =0 ; i < size; i++) {
            cout << edgeList[i].first << " " << edgeList[i].second << endl;
        }
        
    }


