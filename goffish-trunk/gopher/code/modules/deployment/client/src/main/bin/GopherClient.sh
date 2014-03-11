#!/bin/bash

if [ $# -lt 1 ] ; then
  echo "Invalid arguments . command GopherClient.sh gopherConfig gofsConfig graphId jarFile clzz data (optional)iteration"
  exit 1
fi


PRG="$0"

while [ -h "$PRG" ]; do
  ls=`ls -ld "$PRG"`
  link=`expr "$ls" : '.*-> \(.*\)$'`
  if expr "$link" : '.*/.*' > /dev/null; then
    PRG="$link"
  else
    PRG=`dirname "$PRG"`/"$link"
  fi
done

# Get standard environment variables
PRGDIR=`dirname "$PRG"`


# Only set PRG_HOME if not already set
[ -z "$PRG_HOME" ] && PRG_HOME=`cd "$PRGDIR/.." ; pwd`

echo Using HOME DIR $PRG_HOME
cd $PRG_HOME

LOCALCLASSPATH=.
LOCALCLASSPATH=`echo lib/*.jar | tr ' ' ':'`:$LOCALCLASSPATH

JAVA_OPTS=
# JAVA_OPTS="-Dlog=:WARNING $JAVA_OPTS"

if [ -z "$JAVA_HOME" ] ; then
  JAVA=`/usr/bin/which java`
  if [ -z "$JAVA" ] ; then
    echo "Cannot find JAVA. Please set your PATH or JAVA_HOME."
    exit 1
  fi
  JAVA_BIN=`dirname $JAVA`
  JAVA_HOME=$JAVA_BIN/..
else
  JAVA=$JAVA_HOME/bin/java
fi

echo "Using java version: "
$JAVA -version

MY_JAVA="$JAVA $JAVA_OPTS -cp $LOCALCLASSPATH"
CMD="$MY_JAVA edu.usc.goffish.gopher.impl.client.GopherClient $1 $2 $3 $4 $5 $6 $7"


$CMD
