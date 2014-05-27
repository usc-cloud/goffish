#!/bin/bash

# Test codebase paths
SLOUVAIN_DIR=../sequential
PLOUVAIN_DIR=../parallel
OLOUVAIN_DIR=../openmp

SLOUVAIN_LIB= -lslouvain
PLOUVAIN_LIB= -lplouvain
OLOUVAIN_LIB= -lolouvain

#CC=g++
CC=mpicxx
#OPT_FLAG=-g
OT_FLAG=-O5
CFLAGS= -ansi $(OPT_FLAG) -Wall -fopenmp -I$(SLOUVAIN_DIR) -I$(PLOUVAIN_DIR) -I$(OLOUVAIN_DIR)
LDFLAGS= -ansi -lm -Wall -fopenmp -L$(SLOUVAIN_DIR) $(SLOUVAIN_LIB) -L$(PLOUVAIN_DIR) $(PLOUVAIN_LIB) 
#-L$(OLOUVAIN_DIR) $(OLOUVAIN_LIB)

EXEC=benchmark_serial benchmark_parallel
#benchmark_openmp

all: $(EXEC)

#benchmark : benchmark.o
#	$(CC) -o $@ $^ $(LDFLAGS)

benchmark_serial : benchmark_serial.o benchmark_util.o
	$(CC) -o $@ $^ $(LDFLAGS)

benchmark_parallel : benchmark_parallel.o benchmark_util.o
	$(CC) -o $@ $^ $(LDFLAGS)

benchmark_openmp : benchmark_openmp.o benchmark_util.o
	$(CC) -o $@ $^ $(LDFLAGS)


##########################################
# Generic rules
##########################################

%.o: %.cpp %.h
	$(CC) -o $@ -c $< $(CFLAGS)

%.o: %.cpp
	$(CC) -o $@ -c $< $(CFLAGS)

clean:
	rm -f *.o *~ $(EXEC) 
