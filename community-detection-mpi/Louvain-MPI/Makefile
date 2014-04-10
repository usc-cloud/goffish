#!/bin/bash

CC=g++
CFLAGS= -ansi -O5 -Wall
LDFLAGS= -ansi -lm -Wall
#EXEC=community converter hierarchy
EXEC=convert
OBJ1= graph_binary.o
OBJ2= 	graph.o

all: $(EXEC)

#community : $(OBJ1) main_community.o
#	$(CC) -fopenmp -o $@ $^ $(LDFLAGS)

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
	rm -f *.o *~ $(EXEC)
