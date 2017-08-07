#!/bin/bash
#set -x
#
##_term() {
##  echo "Caught SIGTERM signal!"
##}
#trap "echo Booh!" SIGINT SIGTERM
#echo "pid is $$"
#
#while :			# This is the same as "while true".
#do
#  echo "still here"
#        sleep 2	# This script is not really doing anything.
#done

#asyncRun() {
#    "$@" &
#    pid="$!"
#    trap "echo 'Stopping PID $pid'; kill -SIGTERM $pid" SIGINT SIGTERM
#
#    # A signal emitted while waiting will make the wait command return code > 128
#    # Let's wrap it in a loop that doesn't end before the process is indeed stopped
#    while kill -0 $pid > /dev/null 2>&1; do
#        wait
#    done
#}

if [ "$1" = 'run' ]; then
  go build -o /tmp/geb-client /go/src/gitlab.com/proemergotech/geb-client-go/examples.go
  exec /tmp/geb-client
fi

exec "$@"
