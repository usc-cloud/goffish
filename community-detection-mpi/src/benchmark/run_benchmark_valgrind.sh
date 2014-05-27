#!/bin/bash
#####################################################################
#
# File       : run_benchmark_valgrind.sh
#
# Description: Generates benchmarking results for report
#
#
# Author     : Patrick Small
#
# Date       : April 17, 2014
#
#####################################################################

# Benchmark results file
RFILE_SERIAL=benchmark_serial.txt
RFILE_PARALLEL=benchmark_parallel.txt
RFILE_OPENMP=benchmark_openmp.txt

STDOUT_SERIAL=benchmark_serial_stdout.txt
STDOUT_PARALLEL=benchmark_parallel_stdout.txt

# Graph directory
GRAPH_DIR=../../data/graphs_sf

# Test parameters
#GRAPH_SIZES=(1000)
GRAPH_SIZES=(100000 1000000 2000000 4000000 6000000 8000000 10000000)
#GRAPH_NAME_SIZES=(1k)
GRAPH_NAME_SIZES=(100k 1m 2m 4m 6m 8m 10m)
GRAPH_TYPE=sf
#GRAPH_TYPE=ba
NPROCS=(2 4 8 16 32)
#NPROCS=(2)
THREADCOUNT=1

# Use weighted graphs or not
USEWEIGHT=0

# Recreate result files
echo "#NODES           NPROC      STIME      SMOD" > ${RFILE_SERIAL}
echo "#NODES           NPROC      PTIME      PMOD" > ${RFILE_PARALLEL}
echo "#NODES           NPROC      OTIME      OMOD" > ${RFILE_OPENMP}

# Remove old output log
rm ${STDOUT_SERIAL}
rm ${STDOUT_PARALLEL}


# Run serial tests
#for (( i = 0 ; i < ${#GRAPH_SIZES[@]} ; i=$i+1 ));
#do
#    GRAPHFILE=${GRAPH_DIR}/graph_${GRAPH_TYPE}_${GRAPH_NAME_SIZES[$i]}
#    if [ $USEWEIGHT -eq 0 ]; then
#	WEIGHTFLAG=""
#    else
#	WEIGHTFLAG="-w ${GRAPHFILE}.weights"
#    fi
#    echo "./benchmark_serial ${GRAPH_SIZES[$i]} ${GRAPHFILE}.bin -l -1 ${WEIGHTFLAG}"
#    ./benchmark_serial ${GRAPH_SIZES[$i]} ${GRAPHFILE}.bin -l -1 ${WEIGHTFLAG} >> ${STDOUT_SERIAL}
#done

# Run parallel tests
for (( i = 0 ; i < ${#GRAPH_SIZES[@]} ; i=$i+1 ));
do
    for NPROC in "${NPROCS[@]}";
    do
	GRAPHFILE=${GRAPH_DIR}/graph_${GRAPH_TYPE}_${GRAPH_NAME_SIZES[$i]}_${NPROC}
	if [ $USEWEIGHT -eq 0 ]; then
	    WEIGHTFLAG=""
	else
	    WEIGHTFLAG="-w ${GRAPHFILE}.weights"
	fi
	mpirun -np ${NPROC} valgrind -v --leak-check=full --track-origins=yes ./benchmark_parallel ${GRAPH_SIZES[$i]} ${THREADCOUNT} ${GRAPHFILE} -r ${GRAPHFILE} -l -1 -t ${THREADCOUNT} ${WEIGHTFLAG} >> ${STDOUT_PARALLEL}
    done
done

exit 0
