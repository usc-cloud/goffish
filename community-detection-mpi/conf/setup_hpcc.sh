#!/bin/bash

# PGI compilers
#source /usr/usc/mpich/default/mx-pgi/setup.sh

# GNU compilers
#source /usr/usc/mpich/default/mx-gnu43/setup.sh

# CUDA multinode
#source /usr/usc/cuda/5.0/setup.sh
source /usr/usc/mpich2/default/setup.sh
#source /usr/usc/openmpi/1.6.4/setup.sh

# MATLAB
source /usr/usc/matlab/default/setup.sh

# Python NetworkX
export PYTHONPATH=../../../opt/python/networkx.egg

# CMake/PMetis
#export PATH=/home/rcf-104/patrices/opt/cmake-2.8.12.2/bin:/home/rcf-104/patrices/opt/parmetis-4.0.3/bin:/home/rcf-104/patrices/opt/metis-5.1.0/bin:$PATH
