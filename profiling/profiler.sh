#!/bin/bash

if [[ -z "$1" || -z "$2" ]]; then
	echo "Usage: $0 [PROFILE: cpu/lock/..] [optional: OUTPUT]" && exit
fi

if [ -z "$2" ]; then
	OUTPUT="profiler_$1"
else
	OUTPUT="profiler_$1_$2"
fi

JPS=$(jps)
SERVER_PID=$(printf "$JPS" | grep "Server")
if [[ -z "$SERVER_PID" ]]; then
	SERVER_PID=$(printf "$JPS" | grep "Cluster")
fi
SERVER_PID=$(printf "$SERVER_PID" | cut -d " " -f1)

CMD="async-profiler -d 65 -e $1 -f "output/$OUTPUT.html" "$SERVER_PID""

echo -e "$ $CMD\n"
eval $CMD
