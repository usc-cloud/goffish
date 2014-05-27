#!/bin/bash
#####################################################################
#
# File       : run_benchmark_parallel.sh
#
# Description: Generates benchmarking results for report
#
#
# Author     : Patrick Small
#
# Date       : April 17, 2014
#
#####################################################################

#if [ "$#" -ne 1 ]; then
#    echo "usage  : ./run_benchmark_parallel.sh <threadcount>"
#    exit 1
#fi

# Test parameters
# Graph selection
GRAPH_SIZES=(250000 500000 1000000 2000000 4000000 8000000 16000000)
GRAPH_NAME_SIZES=(250k 500k 1m 2m 4m 8m 16m)
GRAPH_TYPE=sf
# MPI Process and thread configuration
NPROCS=(2 4 8 16 32 64 128)
THREADCOUNTS=(1 1 1 1 1 1 1)


# Graph directory
GRAPH_DIR=../../data/graphs_sf

# Benchmark results file
RFILE_PARALLEL=benchmark_parallel.txt

# STDOUT files
STDOUT_PARALLEL=benchmark_parallel_stdout.txt


# Use weighted graphs or not
USEWEIGHT=0

# Recreate result files
echo "#NODES           NPROC      TC         PTIME      PMOD" > ${RFILE_PARALLEL}

# Remove old output log
rm ${STDOUT_PARALLEL}


# Run parallel tests
for (( i = 0 ; i < ${#GRAPH_SIZES[@]} ; i=$i+1 ));
do
    for (( j = 0 ; j < ${#NPROCS[@]} ; j=$j+1 ));
    #for NPROC in "${NPROCS[@]}";
    do
	GRAPHFILE=${GRAPH_DIR}/graph_${GRAPH_TYPE}_${GRAPH_NAME_SIZES[$i]}_${NPROCS[$j]}
	if [ $USEWEIGHT -eq 0 ]; then
	    WEIGHTFLAG=""
	else
	    WEIGHTFLAG="-w ${GRAPHFILE}.weights"
	fi
	mpirun -np ${NPROCS[$j]} ./benchmark_parallel ${GRAPH_SIZES[$i]} ${THREADCOUNTS[$j]} ${GRAPHFILE} -r ${GRAPHFILE} -l -1 -t ${THREADCOUNTS[$j]} ${WEIGHTFLAG} >> ${STDOUT_PARALLEL}
    done
done

exit 0
