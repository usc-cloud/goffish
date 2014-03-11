#!/bin/bash

if [ $# -lt 4 ] ; then

  echo "Invalid arguments . command CopyArtifacts.sh <JarFilePath> <jar_name> <user> <host1> <host2> ..."
  exit 1
fi

for var in "$@"
do

    if [ $var != $1 -a $var != $2 -a $var != $3  ]; then

        echo "coping $1 to $3@$var:~/"
        scp $1 $3@$var:~/
        echo "Moving the jar $2  to Gopher server location "
        ssh  $3@$var mv ./$2 '$GOFFISH_HOME/gopher-server-2.0/apps/'
    fi

done


