#!/usr/bin/env python

import os
import sys
import array
import getopt
import math
import random
import networkx

# Maximum weight for an edge
MAX_WEIGHT = 100


class GenGraphScript:
    def __init__(self, argv):
        self.argc = len(argv)
        self.argv = argv

    def usage(self):
        print "Usage: " + sys.argv[0] + " [-w] [-g gtype] <nodes> <edges> <outfile>"
        print "Example: " + sys.argv[0] + " 1000 2 graph.txt\n"
        print "\t[-h]       : This help message"
        print "\t[-w]       : Assign random weights"
        print "\t[-g]       : Select graph type (ba only right now)"
        print "\t<nodes>    : Number of nodes"
        print "\t<edges>    : Number of edges for each new node"
        print "\t<outfile>  : Output graph filename"
        sys.exit(1)


    def main(self):
        # Parse options
        try:
            opts, args = getopt.getopt(self.argv[1:], "wg:h", ["weight","graph","help"])
        except getopt.GetoptError, err:
            print str(err)
            self.usage()
            return(1)

        # Defaults
        graphtype = "ba"
        useweight = False

        for o, a in opts:
            if o in ("-h", "--help"):
                self.usage()
                return(0)
            elif o in ("-g", "--graph"):
                graphtype = a
            elif o in ("-w", "--weight"):
                useweight = True
            else:
                print "Invalid option %s" % (o)
                return(1)
        
            
        # Check command-line arguments
        if (len(args) != 3):
            self.usage()
            return(1)

        nodes = int(args[0])
        edges = int(args[1])
        outfile = args[2]
        
        print "Generating %s graph with %d nodes and %d new edges per node" % (graphtype, nodes, edges)
        if (graphtype == "ba"):
            g = networkx.barabasi_albert_graph(nodes, edges)
        else:
            print "Invalid graph type: " + graphtype
            return(1)

        # Assign random weights
        if (useweight):
            print "Assigning weights"
            for (u, v) in g.edges():
                weight = int(random.random() * MAX_WEIGHT) + 1
                g[u][v]['weight'] = weight

            # Save to file
            print "Saving graph to file %s" % (outfile)
            f = open(outfile, "wb")
            for (u,v,w) in g.edges(data=True):
                f.write("%d %d %d\n" % (u+1, v+1, w['weight']));
            #networkx.write_weighted_edgelist(g, f);
            f.close()
        else:
            # Save to file
            print "Saving graph to file %s" % (outfile)
            f = open(outfile, "wb")
            for (u,v) in g.edges(data=False):
                f.write("%d %d\n" % (u+1, v+1));
            #networkx.write_edgelist(g, f, data=False);
            f.close()

        print "Done"
        return 0

    
if __name__ == '__main__':

    prog = GenGraphScript(sys.argv)
    sys.exit(prog.main())

