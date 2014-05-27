#!/bin/bash
#####################################################################
#
# File       : run_benchmark_serial.sh
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
GRAPH_SIZES=(250000 500000 1000000 2000000 4000000 8000000 16000000)
#GRAPH_NAME_SIZES=(1k)
GRAPH_NAME_SIZES=(250k 500k 1m 2m 4m 8m 16m)
GRAPH_TYPE=sf
#GRAPH_TYPE=ba
NPROCS=(2 4 8 16 32 64)
#NPROCS=(2)

# Use weighted graphs or not
USEWEIGHT=0

# Recreate result files
echo "#NODES           NPROC      STIME      SMOD" > ${RFILE_SERIAL}
#echo "#NODES           NPROC      PTIME      PMOD" > ${RFILE_PARALLEL}
#echo "#NODES           NPROC      OTIME      OMOD" > ${RFILE_OPENMP}

# Remove old output log
rm ${STDOUT_SERIAL}
#rm ${STDOUT_PARALLEL}


# Run serial tests
for (( i = 0 ; i < ${#GRAPH_SIZES[@]} ; i=$i+1 ));
do
    GRAPHFILE=${GRAPH_DIR}/graph_${GRAPH_TYPE}_${GRAPH_NAME_SIZES[$i]}
    if [ $USEWEIGHT -eq 0 ]; then
	WEIGHTFLAG=""
    else
	WEIGHTFLAG="-w ${GRAPHFILE}.weights"
    fi
    echo "./benchmark_serial ${GRAPH_SIZES[$i]} ${GRAPHFILE}.bin -l -1 ${WEIGHTFLAG}"
    ./benchmark_serial ${GRAPH_SIZES[$i]} ${GRAPHFILE}.bin -l -1 ${WEIGHTFLAG} >> ${STDOUT_SERIAL}
done

exit 0
