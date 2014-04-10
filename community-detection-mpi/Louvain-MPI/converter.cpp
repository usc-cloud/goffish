/**
 * Some of the code was copied from https://sites.google.com/site/findcommunities/
 */

#include "graph.h"
#include <string.h>
#include <sstream>

using namespace std;

char *infile   = NULL;
char *outfile  = NULL;
char *partition_file  = NULL;
int numberOfPartitions =1;

void
usage(char *prog_name, const char *more) {
  cerr << more;
  cerr << "usage: " << prog_name << " -i input_file -o outfile -p partition file -n number of partitions" << endl << endl;
  exit(0);
}

void
parse_args(int argc, char **argv) {
  for (int i = 1; i < argc; i++) {
    if(argv[i][0] == '-') {
      switch(argv[i][1]) {
      case 'i':
	if (i==argc-1)
	  usage(argv[0], "Infile missing\n");
	infile = argv[i+1];
	i++;
	break;
      case 'o':
	if (i==argc-1)
	  usage(argv[0], "Outfile missing\n");
        outfile = argv[i+1];
	i++;
	break;
      case 'p' :
	if (i==argc-1)
	  usage(argv[0], "partition missing\n");
        partition_file = argv[i+1];
	i++;
	break;
      case 'n' :
        if (i==argc-1)
                usage(argv[0], "number of partitions missing\n");
	numberOfPartitions = atoi(argv[i+1]);
        i++;
	break;
      default:
	usage(argv[0], "Unknown option\n");
      }
    } else {
      usage(argv[0], "More than one filename\n");
    }
  }
  if (infile==NULL || outfile==NULL)
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
    
  ifstream finput;
  finput.open(infile,fstream::in);

  vector<pair<int,int> > edgeList;  
  int nb_links=0;

  while (!finput.eof()) {
      unsigned int src,dest;      
      finput >> src >> dest;
      
      edgeList.push_back(make_pair(src,dest));
      nb_links++;
      
  }
   finput.close();
   
   vector<int> partitionMap;
   ifstream fpartition;
   finput.open(partition_file,fstream::in);
   
   int vid = 0;
   while(!fpartition.eof()) {
       int partition;
       fpartition >> partition;     
       partitionMap[vid++] = partition;      
   }
   
   
   
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
   
   
   
   vector<vector<pair<int,int> > > partitions;
   
   vector<vector<pair<int,int> > > remoteEdges;
   
   for(int i=0; i < edgeList.size(); i++) {
        
       int source = edgeList[i].first;
       int sink = edgeList[i].second;
       
       if( partitionMap[source -1] == partitionMap[sink -1]) {
           //in same partition
           partitions[partitionMap[source -1]].push_back(edgeList[i]);
       } else {
           //in different partitions
           remoteEdges[partitionMap[source -1]].push_back(edgeList[i]);     
       }
              
   }
   
   vector<pair<int,int> > oldToNewMap;
   oldToNewMap.resize(vid);
   
   for(int i=0; i< numberOfPartitions; i++) {
       vector<int> map;
       int vid =1;
       for(int j=0;j < partitions[i].size(); j++) {
           
          int source = partitions[i][j].first;
          int sink = partitions[i][j].second;
          
          if(map[source] != 0) {
              source = map[source];
          } else {
              
              map[source] = vid;
              oldToNewMap[source] = make_pair(vid,i);
              source = vid++;
          }
          
          if(map[sink] != 0) {
              sink = map[sink];
          } else {
              map[sink] = vid;
              oldToNewMap[sink] = make_pair(vid,i);
              sink = vid++;
          }
           partitions[i][j].first = source;
           partitions[i][j].second = sink;   
       
       }
       
       
       for(int i =0; i< numberOfPartitions; i++) {
           ofstream fremoteList;
           string s(outfile);
           stringstream ss;
           ss << i;
           string remoteName = s + "_" + ss.str() + ".remote";
            char *cstr = new char[remoteName.length() + 1];
            strcpy(cstr, remoteName.c_str());
           fremoteList.open(cstr,fstream::out);
           delete [] cstr;
           for(int j=0; j< remoteEdges[i].size(); j++) {
               int source = remoteEdges[i][j].first;
               int sink = remoteEdges[i][j].second;               
               fremoteList << source << " " << oldToNewMap[sink].first << "," << oldToNewMap[sink].first  << endl; 
           }
           
           fremoteList.close();
           
           
           
           for(int j=0; j < partitions[i].size(); i++) {
               
               Graph g(partitions[i]);
               g.clean(UNWEIGHTED);
               string s(outfile);
               stringstream ss;
               ss << i;
               string sout = s + "_" + ss.str() + ".bin";
               char *cstr = new char[sout.length() + 1];
                strcpy(cstr, sout.c_str());
                
               g.display_binary(cstr,NULL,UNWEIGHTED);
               delete [] cstr;
           }
       }
       
       
   
   }

   
   
   
   
   
   
    return 0;
}

