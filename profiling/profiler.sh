#/bin/bash

if [[ -z "$1" || -z "$2" ]]; then
	echo "Usage: $0 [PROFILE: cpu/lock/..] [optional: OUTPUT]" && exit
fi

if [ -z "$2" ]; then
	OUTPUT="profiler_$1"
else
	OUTPUT="profiler_$1_$2"
fi

SERVER_PID=$(jps | grep "Server" | cut -d " " -f1)

CMD="async-profiler -d 60 -e $1 -f "output/$OUTPUT.html" "$SERVER_PID""

echo -e "$ $CMD\n"
eval $CMD

