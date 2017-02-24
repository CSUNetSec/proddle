PREFIXES=(
    "https://www."
    "http://www."
    "https://"
    "http://"
)

for PREFIX in "${PREFIXES[@]}"; do
    CURL_RESULT=`curl -L $PREFIX$1 -m 30`
    if [ $? -eq 0 ] && [ ! -z "$CURL_RESULT" ]; then
        echo "SUCCESS $PREFIX$1"

        #increment counter
        COUNT=$[COUNT+1]
        break
    else
        echo "FAIL $DOMAIN"
    fi
done
