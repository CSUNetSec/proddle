#!/bin/bash
TMP_DIR="/tmp"
PREFIXES=(
    "https://www."
    "http://www."
)

if [ $# -ne 1 ]; then
    echo "Usage: $0 COUNT"
    exit 1
fi

#download alexa top 1 million domains
wget -O $TMP_DIR/top-1m.csv.zip http://s3.amazonaws.com/alexa-static/top-1m.csv.zip
unzip -p $TMP_DIR/top-1m.csv.zip > $TMP_DIR/top-1m.csv

#read file
COUNT=0
while read LINE
do
    #parse domain
    DOMAIN=`echo $LINE | cut -f 2 -d ','`

    for PREFIX in "${PREFIXES[@]}"; do
        curl -L $PREFIX$DOMAIN >> /dev/null
        if [ $? -eq 0 ]; then
            ./../../yogi/target/debug/yogi operation add http-get $DOMAIN $PREFIX$DOMAIN -t core -t http
            echo "SUCCESS $DOMAIN"

            #increment counter
            COUNT=$[COUNT+1]
            break
        else
            echo "FAIL $DOMAIN"
        fi
    done

    #check counter
    if [ $COUNT == $1 ];
    then
        break
    fi
done < $TMP_DIR/top-1m.csv

#perform some cleanup
rm $TMP_DIR/top-1m.csv*
