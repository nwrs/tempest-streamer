#!/bin/bash
echo Attemping to stop Tempest Streamer...
pid=`ps -ef | grep -i java | grep -i tempest-streamer | grep -v grep | awk '{print $2}'`
if [[ $pid ]]
then
 echo "Tempest Streamer running [$pid]"
 echo "Killing..."
 result=`kill -9 $pid`
 if [[ $result -eq 0 ]]
 then
    echo "Stopped."
 else
    echo "Failed to kill $pid"
 fi
else
 echo "Not Running."
fi

