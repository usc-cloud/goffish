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
GRAPH_EDGES=2

# Graph directory
GRAPH_DIR=../../../data/graphs

# Use weighted graphs or not
USEWEIGHT=0

# Remove old graphs
rm ${GRAPH_DIR}/*.txt
rm ${GRAPH_DIR}/*.bin
rm ${GRAPH_DIR}/*.weights

# Create the graphs
for (( i = 0 ; i < ${#GRAPH_SIZES[@]} ; i=$i+1 ));
do
    OUTFILE=${GRAPH_DIR}/graph_${GRAPH_TYPE}_${GRAPH_NAME_SIZES[$i]}
    echo "Saving ${GRAPH_TYPE} graph with ${GRAPH_SIZES[$i]} nodes to ${OUTFILE}:"
    if [ $USEWEIGHT -eq 0 ]; then
	WEIGHTFLAG=""
    else
	WEIGHTFLAG="-w"
    fi
    ./gengraph.py ${WEIGHTFLAG} -g ${GRAPH_TYPE} ${GRAPH_SIZES[$i]} ${GRAPH_EDGES} ${OUTFILE}.txt

    if [ $USEWEIGHT -eq 0 ]; then
	WEIGHTFLAG=""
    else
	WEIGHTFLAG="-w ${OUTFILE}.weights"
    fi
    ../../sequential/convert -i ${OUTFILE}.txt -o ${OUTFILE}.bin ${WEIGHTFLAG}
done

