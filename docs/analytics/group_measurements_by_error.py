#!/usr/bin/python2
import bson
import sys

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print "Usage: ", sys.argv[0], "measurement-filename success_rate_file"
        sys.exit(1)

    success_bucket_floors = [0, 20, 40, 60, 80, 100]

    #read success rates
    domain_map = {}
    with open(sys.argv[2]) as success_rate_file:
        for line in success_rate_file:
            fields = line.split()
            failure_rate = float(fields[4]) / (float(fields[1]) + float(fields[4])) * 100

            index = 1 
            while True:
                if index >= len(success_bucket_floors):
                    break
                
                if failure_rate < success_bucket_floors[index]:
                    break

                index += 1

            domain_map[fields[0]] = success_bucket_floors[index - 1]
            #print "adding", fields[0], " ", success_bucket_floors[index-1]

    #read measurements
    error_map = {}
    with open(sys.argv[1]) as measurement_file:
        iter = bson.decode_file_iter(measurement_file)
        for document in iter:
            #get bucket map using error code
            error_message = document["measurement_error_message"]
            error_fields = error_message.split()
            error_code = int(error_fields[0].replace("[","").replace("]",""))

            if error_code not in error_map:
                bucket_map = {}
                for bucket_floor in success_bucket_floors:
                    bucket_map[bucket_floor] = 0

                error_map[error_code] = bucket_map 

            bucket_map = error_map[error_code]

            #increment counter for bucket
            measurement_domain = document["measurement_domain"]
            bucket_floor = domain_map[measurement_domain]
            bucket_map[bucket_floor] += 1

    #print groupings
    #for error_code, bucket_map in error_map.iteritems():
    #    for bucket_floor, count in bucket_map.iteritems():
    #        print "%s,%d,%d" % (error_code.replace("[","").replace("]",""), bucket_floor, count)

    #print groupings
    error_codes = error_map.keys()
    error_codes.sort()
    print error_codes
    for bucket_floor in success_bucket_floors:
        print bucket_floor,
        for error_code in error_codes:
            bucket_map = error_map[error_code]
            print bucket_map[bucket_floor],

        print ""
