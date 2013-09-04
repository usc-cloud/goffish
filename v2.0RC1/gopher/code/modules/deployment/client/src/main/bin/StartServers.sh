#!/bin/bash

if [ $# -lt 4 ] ; then
  echo "Invalid arguments . command StartServers.sh user managerHost coordinatorHost graphId dataURI1 dataURI2 .."
  exit 1
fi
managerHost=$2
coordinatorHost=$3

echo "Starting manager @ $2"

ssh $1@$2 'cd $GOFFISH_HOME/gopher-server-2.0/bin;./manager.sh' $managerHost '`</dev/null` >manager.out 2>&1 &'
echo "Starting coordinator @ $3"
ssh $1@$2 'cd $GOFFISH_HOME/gopher-server-2.0/bin;./coordinator.sh' $coordinatorHost '`</dev/null` >coordinator.out 2>&1 &'

echo waiting...
sleep 5s;
echo startin containers ...
i=0
managerPort=45001
for var in "$@"
do
    #echo $i
    if [ $i -gt 3 ]; then
        dataHost=`echo $var|cut -d'/' -f3`
        echo "Starting  data node container @$dataHost"
        echo "ssh $dataHost 'cd $GOFFISH_HOME/gopher-server-2.0/bin;./container.sh $2 $managerPort $4 $var `</dev/null` >container.out 2>&1 &'"
        ssh $dataHost 'cd $GOFFISH_HOME/gopher-server-2.0/bin;./container.sh' $2 $managerPort $4 $var '`</dev/null` >container.out 2>&1 &'
   fi
    i=$((i+1))

done