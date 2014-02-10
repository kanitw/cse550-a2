#!/bin/bash

# make sure we kill existing servers and clients
pkill -f PaxosClient
pkill -f PaxosServer


# sleep to make sure that they are dead!
sleep 2

python PaxosServer.py 1 5 0 2 &
python PaxosServer.py 2 5 0 3 &


for i in {3..5}
do
  python PaxosServer.py $i 5 0 0 &
done

sleep 1

python PaxosClient.py 1 "lock_a sleep_5 unlock_a" &
python PaxosClient.py 2 "sleep_3 lock_a sleep_2 unlock_a" &
