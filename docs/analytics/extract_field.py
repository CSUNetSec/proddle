#!/usr/bin/python2
import collections
import bson
import sys

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print "Usage: ", sys.argv[0], "field filename"
        sys.exit(1)

    file = open(sys.argv[2])
    iter = bson.decode_file_iter(file)
    for document in iter:
        print document[sys.argv[1]]
