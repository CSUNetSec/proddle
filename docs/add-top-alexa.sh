#!/bin/bash
TMP_DIR="/tmp"

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

    #echo $DOMAIN
    ./../target/debug/yogi operation add -i 14400 http-get.py $DOMAIN

    #increment counter
    COUNT=$[COUNT+1]
    if [ $COUNT == $1 ];
    then
        break
    fi
done < $TMP_DIR/top-1m.csv

#perform some cleanup
rm $TMP_DIR/top-1m.csv*
