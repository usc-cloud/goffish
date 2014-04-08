/**
 * Some of the code was copied from https://sites.google.com/site/findcommunities/
 */

#include "graph.h"

using namespace std;

char *infile   = NULL;
char *outfile  = NULL;
char *partition_file  = NULL;
int nummberOfPartitions =1;

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
	nummberOfPartitions = atoi(argv[i+1]);
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

  ifstream finput;
  finput.open(filename,fstream::in);

  int nb_links=0;

  while (!finput.eof()) {
  
  }
    
    return 0;
}

