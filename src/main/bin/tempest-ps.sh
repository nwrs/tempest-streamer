#!/bin/bash
echo Checking running status of Tempest Streamer...
pid=`ps -ef | grep -i java | grep -i tempest-streamer | grep -v grep | awk '{print $2}'`
if [[ $pid ]]
then
 echo "Tempest Streamer running [$pid]"
else
 echo "Not Running."
fi