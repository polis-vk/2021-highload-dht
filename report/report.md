./profiler.sh -d 120 -e lock -f hw2lock_get.html 712  
wrk -c 64 -t 3 -d 1m -R 10000 -L -s get.lua http://localhost:8080
