#!/bin/bash

CC=g++
MPI=mpic++
CFLAGS= -ansi -O5 -Wall
LDFLAGS= -ansi -lm -Wall
#EXEC=community converter hierarchy
EXEC=community convert
OBJ1= graph_binary.o community.o
OBJ2= 	graph.o

all: $(EXEC)

community : $(OBJ1) community-main.o
	$(MPI) -fopenmp -o $@ $^ $(LDFLAGS)

convert : $(OBJ2) converter.o
	$(CC) -fopenmp -o $@ $^ $(LDFLAGS)

#hierarchy : main_hierarchy.o
#	$(CC) -fopenmp -o $@ $^ $(LDFLAGS)

##########################################
# Generic rules
##########################################

%.o: %.cpp %.h
	$(MPI) -fopenmp -o $@ -c $< $(CFLAGS)

%.o: %.cpp
	$(MPI) -fopenmp -o $@ -c $< $(CFLAGS)

clean:
	rm -f *.o *~ $(EXEC)
