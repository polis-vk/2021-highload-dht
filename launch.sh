#!/bin/bash

echo "Removing temporary directories..."
rm -rf /tmp/juni* || rm -rf /tmp/high*

echo "Stopping not finished Servers and Clusters..."
kill $(jps -l | grep -e "Server" -e "Cluster" | cut -d " " -f 1) > /dev/null 2>&1

echo "Running gradlew..."
./gradlew run