#!/bin/bash
if [ $# -ne 3 ]; then
    echo "Usage: $0 FILENAME USERNAME PASSWORD"
    exit 1
fi

FILENAME=$1
USERNAME=$2
PASSWORD=$3

SSL_CA_CERT="/etc/ssl/mongodb/cacert.pem"
SSL_CERT="/etc/ssl/mongodb/proddle.crt"
SSL_KEY="/etc/ssl/mongodb/proddle.key"
MONGO_HOST="mongo1.netsec.colostate.edu"

while read LINE; do
    read RANK DOMAIN URL <<< $LINE

    echo "$RANK,$DOMAIN,$URL"
    ./../../yogi/target/debug/yogi -c $SSL_CA_CERT -e $SSL_CERT -k $SSL_KEY -u $USERNAME -p $PASSWORD -I $MONGO_HOST operation add http-get $DOMAIN $URL -t core -t http
done <$FILENAME
