#!/usr/bin/python2
import bson
import sys

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print "Usage: ", sys.argv[0], "field filename"
        sys.exit(1)

    #read measurements
    with open(sys.argv[2]) as measurement_file:
        iter = bson.decode_file_iter(measurement_file)
        for document in iter:
            print document[sys.argv[1]]
