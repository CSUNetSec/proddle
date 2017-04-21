#!/usr/bin/python2
import sys

import dbscan_cluster

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print "Usage: %s filename k" % sys.argv[0]
        sys.exit(1)

    #parse parameters
    filename = sys.argv[2]
    k = int(sys.argv[2])

    #compute distance matrix
    measurements, distance_matrix = dbscan_cluster.build_distance_matrix(sys.argv[1])

    #grab k mininimum distances for each point
    k_distances = []
    for i in range(0, len(measurements)):
        distances = sorted(distance_matrix[i])

        for j in range(0, k):
            k_distances.append(distances[j])

    k_distances.sort()
    for i in range(0, len(k_distances)):
        print "%d\t%f" % (i, k_distances[i] / 100 * 8)
