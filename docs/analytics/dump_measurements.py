#!/usr/bin/python2
import bson
import pymongo
import ssl
import sys
from urllib import quote_plus

if __name__ == "__main__":
    if len(sys.argv) != 10:
        print "Usage: ", sys.argv[0], "ca cert key username password host min-ts max-ts output-filename"
        sys.exit(1)

    #connect to mongodb
    #uri = "mongodb://%s:%s@%s/proddle" % (quote_plus(sys.argv[4]), quote_plus(sys.argv[5]), sys.argv[6])
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
        {"$match": {"timestamp": {"$gte": int(sys.argv[7]), "$lte": int(sys.argv[8])}, "remaining_attempts": 0, "measurement_error_message": {"$exists": "true"}}},
        {"$project":{"headers": 0}}
    ]

    output_file = open(sys.argv[9], "wb")
    for document in db.measurements.aggregate(pipeline):
         output_file.write(bson.BSON.encode(document))
