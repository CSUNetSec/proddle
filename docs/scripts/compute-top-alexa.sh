#!/bin/bash
if [ $# -ne 1 ]; then
    echo "Usage: $0 COUNT"
    exit 1
fi

TMP_DIR="/tmp"
PREFIXES=(
    "https://"
    "https://www."
    "http://"
    "http://www."
)

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
	CURL_RESULT=`python http-get.py $PREFIX$DOMAIN`
        if [ $? -eq 0 ] && [ ! -z "$CURL_RESULT" ] && [[ $CURL_RESULT = *\"error\":false* ]]; then
            URL=`echo $CURL_RESULT | jq '.effective_url' | tr -d '"'`
            echo "SUCCESS $COUNT : $DOMAIN ($URL)"

            echo -e "$COUNT\t$DOMAIN\t$PREFIX$DOMAIN\t$URL" >> curl-results.txt

            #increment counter
            COUNT=$[COUNT+1]
            break
        else
            echo "FAIL $PREFIX$DOMAIN"
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
