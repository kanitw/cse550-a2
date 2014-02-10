#!/bin/bash

# make sure we kill existing servers and clients
pkill -f PaxosClient
pkill -f PaxosServer


# sleep to make sure that they are dead!
sleep 2

for i in {1..5}
do
  python PaxosServer.py $i 5 0 0&
done

sleep 1

python PaxosClient.py 1 "lock_a sleep_2 lock_b sleep_1 unlock_a unlock_b" &
python PaxosClient.py 2 "sleep_1 lock_b sleep_1 unlock_b" &
