#!/usr/bin/python2
import bson
import pymongo
import ssl
import sys
from urllib import quote_plus

if __name__ == "__main__":
    if len(sys.argv) != 9:
        print "Usage: ", sys.argv[0], "ca cert key username password host min-ts max-ts"
        sys.exit(1)

    #connect to mongodb
    client = pymongo.MongoClient(sys.argv[6],
            ssl = True,
            ssl_ca_certs = sys.argv[1],
            ssl_certfile = sys.argv[2],
            ssl_keyfile = sys.argv[3])

    #authenticate
    db = client.proddle
    db.authenticate(sys.argv[4], sys.argv[5], source="proddle")

    #query for failures in time range 
    pipeline = [
        {"$match": {"timestamp": {"$gte": int(sys.argv[7]), "$lte": int(sys.argv[8])}}},
        {"$project":{"headers": 0}}
    ]

    domains = {}
    for document in db.measurements.aggregate(pipeline):
        measurement_domain = document["measurement_domain"]

        if measurement_domain not in domains:
            domains[measurement_domain] = [0,0,0,0]

        measurement_counts = domains[measurement_domain]
        if "measurement_error_message" in document:
            remaining_attempts = document["remaining_attempts"]
            if remaining_attempts == 2:
                measurement_counts[1] += 1
            elif remaining_attempts == 1:
                measurement_counts[2] += 1
            elif remaining_attempts == 0:
                measurement_counts[3] += 1
        else:
            measurement_counts[0] += 1

    for domain, measurement_counts in domains.iteritems():
        print domain, measurement_counts[0], measurement_counts[1], measurement_counts[2], measurement_counts[3]
