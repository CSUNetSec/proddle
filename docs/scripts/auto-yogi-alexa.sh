#!/bin/bash
if [ $# -ne 1 ]; then
    echo "Usage: $0 USERNAME PASSWORD COUNT"
    exit 1
fi

USERNAME=$1
PASSWORD=$2

SSL_CA_CERT="/etc/ssl/mongodb/cacert.pem"
SSL_CERT="/etc/ssl/mongodb/proddle.crt"
SSL_KEY="/etc/ssl/mongodb/proddle.key"
MONGO_HOST="mongo1.netsec.colostate.edu"
TMP_DIR="/tmp"

#download alexa top 1 million domains
wget -O $TMP_DIR/top-1m.csv.zip http://s3.amazonaws.com/alexa-static/top-1m.csv.zip
unzip -p $TMP_DIR/top-1m.csv.zip > $TMP_DIR/top-1m.csv

#read file
COUNT=0
while read LINE
do
    #parse domain
    DOMAIN=`echo $LINE | cut -f 2 -d ','`

    #add with yogi
    ./../../yogi/target/debug/yogi -c $SSL_CA_CERT -e $SSL_CERT -k $SSL_KEY -u $USERNAME -p $PASSWORD -I $MONGO_HOST operation add HttpGet $DOMAIN -t core -t http -p "timeout|30"

    #check counter
    COUNT=$[COUNT+1]
    if [ $COUNT == $3 ];
    then
        break
    fi
done < $TMP_DIR/top-1m.csv

#perform some cleanup
rm $TMP_DIR/top-1m.csv*
