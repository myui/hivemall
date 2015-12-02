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
mvn clean package -Dskiptests=true -Dmaven.test.skip=true
