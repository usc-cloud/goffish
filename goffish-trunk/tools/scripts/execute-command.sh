#!/bin/bash

if [ $# -lt 2 ] ; then
  echo "Invalid arguments . command execute_command.sh command user1@host1 user2@host2 user3@host3 ..."
  exit 1
fi

i=0

for var in "$@"
do
  echo $i
  if [ $i -gt 0 ]; then
  	echo "ssh $var $1"
	ssh $var $1	 
  fi	
  i=$((i+1))
done
