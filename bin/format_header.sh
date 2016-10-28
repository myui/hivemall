#!/bin/sh

if [ "$HIVEMALL_HOME" == "" ]; then
  if [ -e ../bin/${0##*/} ]; then
    HIVEMALL_HOME=".."
  elif [ -e ./bin/${0##*/} ]; then
    HIVEMALL_HOME="."
  else
    echo "env HIVEMALL_HOME not defined"
    exit 1
  fi
fi

cd $HIVEMALL_HOME
HIVEMALL_HOME=`pwd`

mvn license:format

cd $HIVEMALL_HOME/spark/spark-common
mvn license:format -P spark-2.0

cd $HIVEMALL_HOME/spark/spark-1.6
mvn license:format -P spark-1.6

cd $HIVEMALL_HOME/spark/spark-2.0
mvn license:format -P spark-2.0
