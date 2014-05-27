#!/usr/bin/env python

import os
import sys
import array
import getopt
import math
import random


class Txt2MetisScript:
    def __init__(self, argv):
        self.argc = len(argv)
        self.argv = argv


    def file_length(self, fname):
        nodes = 0
        edges = 0
        with open(fname) as f:
            for i, l in enumerate(f):
                tokens = l.split()
                v1 = int(tokens[0])
                v2 = int(tokens[1])
                nodes = int(tokens[0])
                if (v2 > v1):
                    edges = edges + 1
        return [nodes, edges]


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
            # Read from file and translate line by line
            print "Scanning graph file %s for statistics" % (infile)
            fileinfo = self.file_length(infile)
            print "Found %d nodes and %d edges" % (fileinfo[0], fileinfo[1])

            print "Translating graph file %s to %s" % (infile, outfile)
            fin = open(infile, "rb")
            fout = open(outfile, "wb")
            if (useweight):
                fout.write("%s %s 1" % (fileinfo[0], fileinfo[1]));
            else:
                fout.write("%s %s" % (fileinfo[0], fileinfo[1]));

            curnode = 0
            for line in fin:
                tokens = line.split()
                v1 = int(tokens[0])
                v2 = int(tokens[1])
                if (v2 == v1):
                    continue;

                if (useweight):
                    w = int(tokens[2])
                    if (v1 != curnode):
                        fout.write('\n%d %d' % (v2, w));
                    else:
                        fout.write(' %d %d' % (v2, w));
                else:
                    if (v1 != curnode):
                        fout.write('\n%d' % (v2));
                    else:
                        fout.write(' %d' % (v2));

                curnode = v1;

            fin.close()
            fout.close();

        except:
            print "ERROR: Failed to read input graph, check that it exists and you are using the correct weight (-w) option."
            return(1)

        print "Done"
        return 0

    
if __name__ == '__main__':

    prog = Txt2MetisScript(sys.argv)
    sys.exit(prog.main())

