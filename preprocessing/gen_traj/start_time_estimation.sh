#!/bin/bash

for t in {0..90};
do
    i=`expr $t \* 300`
    j=`expr $i + 300`
    p=`expr $i + 1`
    q=`expr $j + 1`
    # echo $i $j
    nohup python time_estimation.py $i $j > time_estimation.log &
done