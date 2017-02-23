#!/bin/bash
if [ $# -ne 3 ]; then
    echo "Usage: $0 COUNT USERNAME PASSWORD"
    exit 1
fi

TMP_DIR="/tmp"
PREFIXES=(
    "https://www."
    "http://www."
    "https://"
    "http://"
)

USERNAME=$2
PASSWORD=$3
SSL_CA_CERT="/etc/ssl/mongodb/cacert.pem"
SSL_CERT="/etc/ssl/mongodb/proddle.crt"
SSL_KEY="/etc/ssl/mongodb/proddle.key"
MONGO_HOST="mongo1.netsec.colostate.edu"

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
        CURL_RESULT=`curl -L $PREFIX$DOMAIN -m 20`
        if [ $? -eq 0 ] && [ ! -z "$CURL_RESULT" ]; then
            ./../../yogi/target/debug/yogi -c $SSL_CA_CERT -e $SSL_CERT -k $SSL_KEY -u $USERNAME -p $PASSWORD -I $MONGO_HOST operation add http-get $DOMAIN $PREFIX$DOMAIN -t core -t http
            echo "SUCCESS $DOMAIN"
            echo $PREFIX$DOMAIN > curl-results/$COUNT-$DOMAIN
            echo $CURL_RESULT >> curl-results/$COUNT-$DOMAIN

            #increment counter
            COUNT=$[COUNT+1]
            echo "COUNT:$COUNT"
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
