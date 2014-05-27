
/* Include files */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <math.h>
#include <mpi.h>
#include <string>
#include <iostream> 

#include "lib_slouvain.h"
#include "lib_plouvain.h"
#include "lib_olouvain.h"

/* Number of tests to run */
//#define NUM_TEST 10

/* Function prototypes */
double read_timer();

/* Test driver */
int main(int argc, char **argv)
{
  FILE *fp = NULL;
  //int i;
  int numnodes, numprocs, rank = 0;
  double seconds = 0, selapsed = 0, pelapsed = 0, oelapsed = 0;
  double smod = 0, pmod = 0, omod = 0;

  MPI_Init(&argc, &argv);
  MPI_Comm_size(MPI_COMM_WORLD, &numprocs);
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);

  if (rank == 0) {
    numnodes = atoi(argv[1]);
    std::cout << "Testing graph " << argv[2] << " (" 
	      << numnodes << ") on " << numprocs 
	      << " nodes" << std::endl;

    fp = fopen("benchmark.txt", "a");

    std::cout << "Executing sequential louvain" << std::endl;

    /* Run serial */
    seconds = read_timer();
    smod += slouvain_main(argc-1, &(argv[1]));
    selapsed += (read_timer() - seconds);

    /* Run openmp */
    seconds = read_timer();
    //omod += olouvain_main(argc-1, &(argv[1]));
    oelapsed += (read_timer() - seconds);

  }

  MPI_Barrier(MPI_COMM_WORLD);

  if (rank == 0) {
    std::cout << "Executing parallel louvain" << std::endl;
    seconds = read_timer();
  }

  /* Run parallel */
  pmod += plouvain_main(argc-1, &(argv[1]));
  MPI_Barrier(MPI_COMM_WORLD);
  if (rank == 0) {
    pelapsed += (read_timer() - seconds);
  }

  if (rank == 0) {
    fprintf(fp, "%10d\t%d\t%8.4lf\t%8.4lf\t%8.4lf\t%8.4lf\t%8.4lf\t%8.4lf\n", 
	    numnodes, numprocs, selapsed, smod, oelapsed, omod, pelapsed, pmod);
    fclose(fp);
    std::cout << "Done" << std::endl;
  }

  MPI_Finalize();

  return 0;
}


double read_timer()
{
  static bool initialized = false;
  static struct timeval start;
  struct timeval end;
  if( !initialized ) {
    gettimeofday( &start, NULL );
    initialized = true;
  }
  
  gettimeofday( &end, NULL );
  
  return (end.tv_sec - start.tv_sec) + 1.0e-6 * (end.tv_usec - start.tv_usec);
}
