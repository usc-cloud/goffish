#!/bin/bash

#CC=g++
CC=mpic++
#MPI=mpic++
#OPT_FLAG=-g
OPT_FLAG=-O5
CFLAGS= -ansi $(OPT_FLAG) -Wall
LDFLAGS= -ansi -lm -Wall
#EXEC=community converter hierarchy
EXEC=community convert
OBJ1= graph_binary_better.o graph_binary.o community.o 
OBJ2= 	graph.o
LIBARCHIVE=libplouvain.a

all: $(EXEC) lib

lib : $(OBJ1) lib_plouvain.o
	ar rcs $(LIBARCHIVE) $^	

community : $(OBJ1) community-main.o
	$(CC) -fopenmp -o $@ $^ $(LDFLAGS)

convert : $(OBJ2) converter.o
	$(CC) -fopenmp -o $@ $^ $(LDFLAGS)

#hierarchy : main_hierarchy.o
#	$(CC) -fopenmp -o $@ $^ $(LDFLAGS)

##########################################
# Generic rules
##########################################

%.o: %.cpp %.h
	$(CC) -fopenmp -o $@ -c $< $(CFLAGS)

%.o: %.cpp
	$(CC) -fopenmp -o $@ -c $< $(CFLAGS)

clean:
	rm -f *.o *~ $(EXEC) $(LIBARCHIVE)
