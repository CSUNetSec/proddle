#!/usr/bin/python2
import bson
import math
import numpy
import sklearn
from sklearn import metrics
from sklearn.cluster import DBSCAN
import sys

def compute_distance(document_one, document_two):
    if document_one["measurement_domain"] != document_two["measurement_domain"]:
        return 100

    timestamp_difference = abs(document_one["timestamp"] - document_two["timestamp"]) / 3600.0
    timestamp_score = 0
    #if timestamp_difference >= 0.0 and timestamp_difference <= 1.0:
    #    timestamp_score = math.log(timestamp_difference + 1.00027) / math.log(24.0)
    #else:
    #    timestamp_score = 1.0
    if timestamp_difference >= 8:
        timestamp_score = 1.0
    else:
        timestamp_score = timestamp_difference / 8

    return timestamp_score * 100

def compute(distance_matrix, eps, min_samples):
    #perform clustering
    db = DBSCAN(eps=eps, min_samples=min_samples, metric="precomputed", n_jobs=4).fit(distance_matrix)
    labels = db.labels_

    return labels

def build_distance_matrix(filename):
    #read measurements
    measurements = []
    #count = 300
    with open(filename) as measurement_file:
        iter = bson.decode_file_iter(measurement_file)
        for document in iter:
            #count -= 1
            measurements.append(document)

            #if count == 0:
            #    break

    #compute distance matrix
    distance_matrix = []
    for i in range(0, len(measurements)):
        distance_matrix.append([]);

    for i in range(0, len(measurements) - 1):
        distance_matrix[i].append(100.0)
        for j in range(i + 1, len(measurements)):
            distance = compute_distance(measurements[i], measurements[j])
            distance_matrix[i].append(distance)
            distance_matrix[j].append(distance)

    distance_matrix[len(distance_matrix) - 1].append(100.0)

    return measurements, distance_matrix

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print "Usage: ", sys.argv[0], "filename eps min_samples"
        sys.exit(1)

    measurements, distance_matrix = build_distance_matrix(sys.argv[1])
    labels = compute(distance_matrix, float(sys.argv[2]), int(sys.argv[3]))

    #print measurements and labels
    #for i in range(0, len(measurements)):
    #    label = labels[i]
    #    timestamp = measurements[i]["timestamp"]
    #    domain = measurements[i]["measurement_domain"]
    #    error_fields = measurements[i]["measurement_error_message"].split()
    #    error_code = error_fields[0].replace("[", "").replace("]", "")
    #    vantage = measurements[i]["vantage_hostname"]
    #    ip = measurements[i]["vantage_ip_address"]
    #    print "%d\t%d\t%s\t%s\t%s\t%s" % (label, timestamp, domain, error_code, vantage, ip)

    #print graphml
    print "<graphml>"
    print "\t<key id=\"d0\" for=\"node\" attr.name=\"Modularity Class\" attr.type=\"integer\"/>"
    print "\t<key id=\"d1\" for=\"edge\" attr.name=\"Weight\" attr.type=\"double\"/>"
    print "\t<graph id=\"F\" edgedefault=\"undirected\">"
    for i in range(0, len(measurements)):
        print "\t\t<node id=\"%s\">" % measurements[i]["_id"]
        print "\t\t\t<data key=\"d0\">%d</data>" % labels[i]
        print "\t\t</node>"

    for i in range(0, len(distance_matrix)):
        for j in range(i+1, len(distance_matrix[i])):
            distance = distance_matrix[i][j]
            if distance == 100:
                continue

            print "\t\t<edge source=\"%s\" target=\"%s\">" % (measurements[i]["_id"], measurements[j]["_id"])
            
            weight = (-9.0 / 100.0) * distance + 10.0
            print "\t\t\t<data key=\"d1\">%d</data>" % weight
            print "\t\t</edge>"

    print "\t</graph>"
    print "</graphml>"
