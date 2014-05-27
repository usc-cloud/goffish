#!/bin/bash
#####################################################################
#
# File       : run_binary_networks.sh
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
GRAPH_SIZES=(1000 10000 100000 1000000)
GRAPH_NAME_SIZES=(1k 10k 100k 1m)
GRAPH_TYPE=sf
#GRAPH_EDGES=2
GRAPH_AVG_K=8
GRAPH_MAX_K=20
GRAPH_MU=0.1
NPROCS=(2 4 8)

# Graph directory
GRAPH_DIR=../../../data/graphs

# Use weighted graphs or not
USEWEIGHT=0

# Remove old graphs
rm ${GRAPH_DIR}/*.txt
rm ${GRAPH_DIR}/*.bin
rm ${GRAPH_DIR}/*.weights
rm ${GRAPH_DIR}/*.remote
rm ${GRAPH_DIR}/*.metis
rm ${GRAPH_DIR}/*.part.*

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
	pmetis ${GRAPHFILE}.metis ${NPROC}

        # Run the parallel converter on the partitioned graph
        ../../parallel/convert -i ${GRAPHFILE}.txt -p ${GRAPHFILE}.metis.part.${NPROC} -o ${GRAPHFILE}_${NPROC} -n ${NPROC} ${WEIGHTFLAG}

    done

done

