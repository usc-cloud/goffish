#!/bin/bash
#####################################################################
#
# File       : run_binary_networks_hpcc.sh
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
#GRAPH_SIZES=(100000 1000000 2000000 4000000 6000000 8000000 10000000 12000000 14000000 16000000)
#GRAPH_NAME_SIZES=(100k 1m 2m 4m 6m 8m 10m 12m 14m 16m)
#GRAPH_TYPE=sf
##GRAPH_EDGES=2
#GRAPH_AVG_K=8
#GRAPH_MAX_K=20
#GRAPH_MU=0.1
#NPROCS=(2 4 8 16 32 64 128)

# Graphs to generate
GRAPH_SIZES=(250000 500000)
GRAPH_NAME_SIZES=(250k 500k)
GRAPH_TYPE=sf
#GRAPH_EDGES=2
GRAPH_AVG_K=8
GRAPH_MAX_K=20
GRAPH_MU=0.1
NPROCS=(2 4 8 16 32 64 128)

# Graph directory
GRAPH_DIR=../../../data/graphs_sf

# Use weighted graphs or not
USEWEIGHT=0

# Remove old graphs
#rm ${GRAPH_DIR}/*.txt
#rm ${GRAPH_DIR}/*.bin
#rm ${GRAPH_DIR}/*.weights
#rm ${GRAPH_DIR}/*.remote
#rm ${GRAPH_DIR}/*.metis
#rm ${GRAPH_DIR}/*.part.*

# Create the graphs
for (( i = 0 ; i < ${#GRAPH_SIZES[@]} ; i=$i+1 ));
do
    GRAPHFILE=${GRAPH_DIR}/graph_${GRAPH_TYPE}_${GRAPH_NAME_SIZES[$i]}
    echo "Saving ${GRAPH_TYPE} graph with ${GRAPH_SIZES[$i]} nodes to ${GRAPHFILE}:"
    if [ $USEWEIGHT -eq 0 ]; then
	WEIGHTFLAG=""
    else
	echo "ERROR: These codes do not support weighted graphs"
	exit 1
    fi

    # Generate the graph
    ./benchmark -N ${GRAPH_SIZES[$i]} -k ${GRAPH_AVG_K} -maxk ${GRAPH_MAX_K} -mu ${GRAPH_MU} -minc 20 -maxc 50
    mv network.dat ${GRAPHFILE}.txt
    rm *.dat
    if [ $USEWEIGHT -eq 0 ]; then
	WEIGHTFLAG=""
    else
	WEIGHTFLAG="-w ${GRAPHFILE}.weights"
    fi

    # Convert for serial louvain
    ../../sequential/convert -i ${GRAPHFILE}.txt -o ${GRAPHFILE}.bin ${WEIGHTFLAG}

    # Convert for metis
    echo "./txt2metis.py ${WEIGHTFLAG} ${GRAPHFILE}.txt ${GRAPHFILE}.metis"
    ./txt2metis.py ${WEIGHTFLAG} ${GRAPHFILE}.txt ${GRAPHFILE}.metis

    # Create partitions for each node count
    for NPROC in "${NPROCS[@]}";
    do
	# Partition with metis
	gpmetis ${GRAPHFILE}.metis ${NPROC}

        # Run the parallel converter on the partitioned graph
        ../../parallel/convert -i ${GRAPHFILE}.txt -p ${GRAPHFILE}.metis.part.${NPROC} -o ${GRAPHFILE}_${NPROC} -n ${NPROC} ${WEIGHTFLAG}

    done

done

