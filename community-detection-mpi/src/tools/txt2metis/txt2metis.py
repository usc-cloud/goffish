#!/usr/bin/env python

import os
import sys
import array
import getopt
import math
import random
import networkx


class Txt2MetisScript:
    def __init__(self, argv):
        self.argc = len(argv)
        self.argv = argv

    def usage(self):
        print "Usage: " + sys.argv[0] + " [-w] <in txt graph> <out metis graph>"
        print "Example: " + sys.argv[0] + "../../../data/graphs/graph_ba_1k.txt ../../../data/graph_ba_1k.metis\n"
        print "\t[-h]              : This help message"
        print "\t[-w]              : Input graph has weights"
        print "\t<in txt graph>    : Input networkx txt graph"
        print "\t<out metis graph> : Output metis graph"
        sys.exit(1)


    def main(self):
        # Parse options
        try:
            opts, args = getopt.getopt(self.argv[1:], "wh", ["weight","help"])
        except getopt.GetoptError, err:
            print str(err)
            self.usage()
            return(1)

        # Defaults
        useweight = False;

        for o, a in opts:
            if o in ("-h", "--help"):
                self.usage()
                return(0)
            elif o in ("-w", "--weight"):
                useweight = True
            else:
                print "Invalid option %s" % (o)
                return(1)
            
        # Check command-line arguments
        if (len(args) != 2):
            self.usage()
            return(1)

        infile = args[0]
        outfile = args[1]

        print "Converting graph %s from txt format to metis weighted edge format" % (infile)

        try:
            # Read from file
            print "Reading graph file %s" % (infile)
            f = open(infile, "rb")
            if (useweight):
                g = networkx.read_weighted_edgelist(f, nodetype=int);
            else:
                g = networkx.read_edgelist(f, nodetype=int);
            f.close()
        except:
            print "ERROR: Failed to read input graph, check that it exists and you are using the correct weight (-w) option."
            return(1)

        if ((useweight) and ('weight' not in g.edges(data=True)[0][2])):
            print "ERROR: Weight option specified yet no weights in graph"
            return(1)

        print "Read %d nodes, %d edges" % (g.number_of_nodes(), g.number_of_edges())

        # Sort the nodes in increasing order
        snodes = g.nodes()
        snodes.sort(key=int)

        # Remove self loops
        selflist = []
        for s in snodes:
            if (s in g.neighbors(s)):
                selflist.append(s)

        print "Removing %d self loops" % (len(selflist))
        for s in selflist:
            g.remove_edge(s,s)

        # Save to file
        print "Writing metis graph file %s" % (outfile)
        f = open(outfile, "wb")
        if (useweight):
            f.write("%s %s 1\n" % (g.number_of_nodes(), g.number_of_edges()));
        else:
            f.write("%s %s\n" % (g.number_of_nodes(), g.number_of_edges()));

        # Iterate over each neighbor
        if (useweight):
            for s in snodes:
                for n in g.neighbors(s):
                    f.write(" %10d %3d" % (int(n), g[s][n]['weight']))
                f.write("\n");
        else:
            for s in snodes:
                for n in g.neighbors(s):
                    f.write(" %10d" % (int(n)))
                f.write("\n");
        f.close()

        print "Done"
        return 0

    
if __name__ == '__main__':

    prog = Txt2MetisScript(sys.argv)
    sys.exit(prog.main())

