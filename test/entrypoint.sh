#!/usr/bin/env bash

while true
do
  echo "Waiting for server"

  if curl -isf http://admin:admin@geb_server:15672/api/aliveness-test/%2F > /dev/nul
  then
    break
  fi

  sleep 1
done

exec "$@"
