#!/bin/bash
for (( i = 0; i <= 25; i++ ))
do
	echo "========================================$i============================================"
	time go test -run 2D -race
done  