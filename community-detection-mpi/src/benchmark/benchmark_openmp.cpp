
/* Include files */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <math.h>
#include <mpi.h>
#include <string>
#include <iostream> 

#include "benchmark_util.h"
#include "lib_olouvain.h"

/*  Number of tests to run */
//#define NUM_TEST 10


/* Test driver */
int main(int argc, char **argv)
{
  FILE *fp = NULL;
  //int i;
  int numnodes;
  double seconds = 0, oelapsed = 0;
  double omod;
  
  if (argc < 2) {
    std::cout << "usage: benchmark_serial <numnodes> <louvain arguments>"
	      << std::endl;
    return(1);
  }

  numnodes = atoi(argv[1]);
  std::cout << "Testing graph " << argv[2] << " (" 
	    << numnodes << ")" << std::endl;
  
  fp = fopen("benchmark_openmp.txt", "a");
  
  std::cout << "Executing sequential louvain" << std::endl;
  
  /* Run openmp */
  seconds = read_timer();
  //omod += olouvain_main(argc-1, &(argv[1]));
  oelapsed += (read_timer() - seconds);
  
  fprintf(fp, "%10d\t%d\t%8.4lf\t%8.4lf\n", numnodes, 1, oelapsed, omod);
  fclose(fp);
  std::cout << "Done" << std::endl;

  return 0;
}
