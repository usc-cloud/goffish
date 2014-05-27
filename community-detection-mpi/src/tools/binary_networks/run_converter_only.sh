#!/bin/bash
#####################################################################
#
# File       : run_converter_only.sh
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
GRAPH_SIZES=(10000000 12000000 14000000 16000000)
GRAPH_NAME_SIZES=(10m 12m 14m 16m)
GRAPH_TYPE=sf
#GRAPH_EDGES=2
GRAPH_AVG_K=8
GRAPH_MAX_K=20
GRAPH_MU=0.1
NPROCS=(2 4 8 16 32 64 128)

# Graph directory
GRAPH_DIR=../../../data/graphs_sf


# Graphs to generate
#GRAPH_SIZES=(1000 10000 100000 1000000)
#GRAPH_NAME_SIZES=(1k 10k 100k 1m)
#GRAPH_TYPE=sf
##GRAPH_EDGES=2
#GRAPH_AVG_K=8
#GRAPH_MAX_K=20
#GRAPH_MU=0.1
#NPROCS=(2 4 8)

# Graph directory
#GRAPH_DIR=../../../data/graphs


# Partitioner to use gpmetis or pmetis
PMETIS=gpmetis

# Use weighted graphs or not
USEWEIGHT=0

# Create the graphs
for (( i = 0 ; i < ${#GRAPH_SIZES[@]} ; i=$i+1 ));
do
    GRAPHFILE=${GRAPH_DIR}/graph_${GRAPH_TYPE}_${GRAPH_NAME_SIZES[$i]}

    if [ $USEWEIGHT -eq 0 ]; then
	WEIGHTFLAG=""
    else
	WEIGHTFLAG="-w ${GRAPHFILE}.weights"
    fi

    # Convert for metis
    #echo "./txt2metis.py ${WEIGHTFLAG} ${GRAPHFILE}.txt ${GRAPHFILE}.metis"
    #./txt2metis.py ${WEIGHTFLAG} ${GRAPHFILE}.txt ${GRAPHFILE}.metis

    # Create partitions for each node count
    for NPROC in "${NPROCS[@]}";
    do
	# Partition with metis
	${PMETIS} ${GRAPHFILE}.metis ${NPROC}

        # Run the parallel converter on the partitioned graph
        ../../parallel/convert -i ${GRAPHFILE}.txt -p ${GRAPHFILE}.metis.part.${NPROC} -o ${GRAPHFILE}_${NPROC} -n ${NPROC} ${WEIGHTFLAG}

    done

done

