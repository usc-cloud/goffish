#!/bin/bash
#####################################################################
#
# File       : run_gengraph_hpcc.sh
#
# Description: Generates benchmarking graphs
#
#
# Author     : Patrick Small
#
# Date       : April 17, 2014
#
#####################################################################

# Graphs to generate
GRAPH_SIZES=(1000 10000 100000 1000000 10000000)
GRAPH_NAME_SIZES=(1k 10k 100k 1m 10m)
GRAPH_TYPE=ba
NPROCS=(2 4 8 16 32)


# Graph directory
GRAPH_DIR=../../../data/graphs

# Use weighted graphs or not
USEWEIGHT=0


# Create the metis graph partitions
for (( i = 0 ; i < ${#GRAPH_SIZES[@]} ; i=$i+1 ));
do
    GRAPHFILE=${GRAPH_DIR}/graph_${GRAPH_TYPE}_${GRAPH_NAME_SIZES[$i]}
    echo "Converting graph ${GRAPHFILE} with ${GRAPH_SIZES[$i]} nodes:"
    if [ $USEWEIGHT -eq 0 ]; then
	WEIGHTFLAG=""
    else
	WEIGHTFLAG="-w"
    fi
    echo "./txt2metis.py ${WEIGHTFLAG} ${GRAPHFILE}.txt ${GRAPHFILE}.metis"
    ./txt2metis.py ${WEIGHTFLAG} ${GRAPHFILE}.txt ${GRAPHFILE}.metis
    for NPROC in "${NPROCS[@]}";
    do
	gpmetis ${GRAPHFILE}.metis ${NPROC}

        # Run the parallel converter on the partitioned graph
        ../../parallel/convert -i ${GRAPHFILE}.txt -p ${GRAPHFILE}.metis.part.${NPROC} -o ${GRAPHFILE}_${NPROC} -n ${NPROC} ${WEIGHTFLAG}

    done
done



