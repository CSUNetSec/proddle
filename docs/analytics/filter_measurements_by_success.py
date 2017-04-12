#!/usr/bin/python2
import bson
import sys

if __name__ == "__main__":
    if len(sys.argv) != 5:
        print "Usage: ", sys.argv[0], "measurement-filename success-rate-filename failure_rate output-filename"
        sys.exit(1)

    failure_rate = float(sys.argv[3])

    #read success rates
    exclude_domain = set()
    with open(sys.argv[2]) as success_rate_file:
        for line in success_rate_file:
            fields = line.split()
            total = int(fields[1]) + int(fields[4])
            failure_percent = float(fields[4]) / total

            if failure_percent >= failure_rate:
                exclude_domain.add(fields[0])
                #print fields[0], " - ", failure_percent

    #read measurements
    output_file = open(sys.argv[4], "wb")
    with open(sys.argv[1]) as measurement_file:
        iter = bson.decode_file_iter(measurement_file)
        for document in iter:
            if document["measurement_domain"] in exclude_domain:
                continue

            output_file.write(bson.BSON.encode(document))
