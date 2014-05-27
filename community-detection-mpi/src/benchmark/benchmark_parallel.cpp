
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
#include "lib_plouvain.h"

/* Number of tests to run */
//#define NUM_TEST 10


/* Test driver */
int main(int argc, char **argv)
{
  FILE *fp = NULL;
  //int i;
  int len, numnodes, numthreads, numprocs, rank = 0;
  double seconds = 0, pelapsed = 0;
  double pmod = 0;
  char name[MPI_MAX_PROCESSOR_NAME];
  char filename[256];

  if (argc < 3) {
    std::cout << "usage: benchmark_parallel <numn graph nodes> <num threads> <louvain arguments>"
	      << std::endl;
    return(1);
  }

  MPI_Init(&argc, &argv);
  MPI_Comm_size(MPI_COMM_WORLD, &numprocs);
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Get_processor_name(name, &len);
  printf("Rank %03d of %03d: Running on node %s\n", rank, numprocs, name);

  if (rank == 0) {
    numnodes = atoi(argv[1]);
    numthreads = atoi(argv[2]);
    std::cout << "Testing graph " << argv[2] << " (" 
	      << numnodes << ") on " << numprocs 
	      << " nodes and " << numthreads << " threads" 
	      << std::endl;

    if (numthreads != 1) {
      sprintf(filename, "benchmark_parallel_t%d.txt", numthreads);
    } else {
      sprintf(filename, "benchmark_parallel.txt");
    }

    fp = fopen(filename, "a");

  }

  MPI_Barrier(MPI_COMM_WORLD);

  if (rank == 0) {
    std::cout << "Executing parallel louvain" << std::endl;
    seconds = read_timer();
  }

  /* Run parallel */
  pmod += plouvain_main(argc-2, &(argv[2]));
  MPI_Barrier(MPI_COMM_WORLD);
  if (rank == 0) {
    pelapsed += (read_timer() - seconds);
  }

  if (rank == 0) {
    fprintf(fp, "%10d\t%d\t%d\t%8.4lf\t%8.7lf\n", 
	    numnodes, numprocs, numthreads, pelapsed, pmod);
    fclose(fp);
    std::cout << "Done" << std::endl;
  }

  MPI_Finalize();

  return 0;
}
