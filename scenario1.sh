#!/bin/bash

for i in {1..3}
do
  python PaxosServer.py $i 3 &
done