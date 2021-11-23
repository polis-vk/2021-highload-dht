#!/bin/bash

if [[ $# -eq 0 || "$1" == "-h" ]]; then
	echo "Usage: $0 [get or put] [output file]"
	exit
fi

if [[ $1 == "get" ]]; then
	SCRIPT="./scripts/get.lua"
elif [[ $1 != "put" ]]; then
	echo "check -h" && exit
else
	SCRIPT="./scripts/put.lua"
fi

if [ -z "$2" ]; then
	OUTPUT="wrk2_output_$1"
else
	OUTPUT="wrk2_$2_$1"
fi

mkdir output &>/dev/null
CMD="wrk2 -c 128 -t 8 -d 65s -R 20000 -L -s "$SCRIPT" http://localhost:8080/ >> "output/$OUTPUT.txt""

echo -e "$ $CMD"
echo -e "$ $CMD\n\n" > "output/$OUTPUT.txt"
eval $CMD

cat "output/$OUTPUT.txt"
