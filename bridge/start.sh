#!/bin/bash
while true; do
    #start bridge
    ./target/debug/bridge -u proddle -p 'f@#d4k(YBN:c?u$gVh7W' &

    sleep 7200

    #stop bridge
    echo "RESTARTING BRIDGE"
    for PID in `ps -aux | grep bridge | awk '{print $2}'`; do
        kill $PID
    done
done
