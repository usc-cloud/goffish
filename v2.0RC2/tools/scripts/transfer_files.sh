#!/bin/bash

if [ $# -lt 3 ] ; then
  echo "Invalid arguments . command transfer_files.sh source target user1@host1 user2@host2 user3@host3 ..."
  exit 1
fi

i=0

for var in "$@"
do
  echo $i
  if [ $i -gt 1 ]; then
  	echo "scp $1 $var:$2"
	scp $1 $var:$2	 
  fi	
  i=$((i+1))
done


