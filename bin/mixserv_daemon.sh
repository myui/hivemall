#!/bin/sh

# Hivemall: Hive scalable Machine Learning Library
#
# Copyright (C) 2015 Makoto YUI
# Copyright (C) 2013-2015 National Institute of Advanced Industrial Science and Technology (AIST)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

usage="Usage: mixserv_daemon.sh (start|stop|status)"

# If no args specified, show usage
if [ $# -ne 1 ]; then
  echo $usage
  exit 1
fi

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

# Load global variables
. "$HIVEMALL_HOME/conf/mixserv_env.sh"

# Load a version number from a VERSION file
VERSION=`cat $HIVEMALL_HOME/VERSION`

MIXSERV_PID_DIR="/tmp"
MIXSERV_PID_FILE="$MIXSERV_PID_DIR/hivemall-mixserv-$VERSION-$USER.pid"
MIXSERV_LOG_DIR="$HIVEMALL_HOME/logs"
MIXSERV_LOG_FILE="$MIXSERV_LOG_DIR/hivemall-mixserv-$VERSION-$USER-$HOSTNAME.out"

rotate_mixserv_log() {
  log=$1
  num=5
  if [ -n "$2" ]; then
    num=$2
  fi
  if [ -f "$log" ]; then # rotate logs
    while [ $num -gt 1 ]; do
      prev=`expr $num - 1`
      [ -f "$log.$prev" ] && mv "$log.$prev" "$log.$num"
      num=$prev
    done
  mv "$log" "$log.$num";
  fi
}

# Sanitize log directory
mkdir -p "$MIXSERV_LOG_DIR"
touch "$MIXSERV_LOG_DIR"/.hivemall_test > /dev/null 2>&1
TEST_LOG_DIR=$?
if [ "${TEST_LOG_DIR}" = "0" ]; then
  rm -f "$MIXSERV_LOG_DIR"/.hivemall_test
else
  chown "$USER" "$MIXSERV_LOG_DIR"
fi

# Set default scheduling priority
if [ "$MIXSERV_NICENESS" = "" ]; then
  export MIXSERV_NICENESS="0"
fi

case $1 in

  (start)

    # Check if the MIX server has already run
    if [ -f $MIXSERV_PID_FILE ]; then
      TARGET_ID="$(cat $MIXSERV_PID_FILE)"
      if [[ $(ps -p "$TARGET_ID" -o comm=) =~ "java" ]]; then
        echo "MIX server has already run as process ${TARGET_ID}"
        exit 0
      fi
    fi

    JARFILE="$HIVEMALL_HOME/target/hivemall-mixserv-$VERSION-fat.jar"
    if [ -f "$JARFILE" ]; then
      # Launch a MIX server
      rotate_mixserv_log "$MIXSERV_LOG_FILE"
      echo "starting a MIX server, logging to ${MIXSERV_LOG_FILE}"

      nohup nice -n "$MIXSERV_NICENESS" java ${MIXSERV_JMXOPTS} ${MIXSERV_VMOPTS} \
        -jar "$JARFILE" ${MIXSERV_OPS} > "$MIXSERV_LOG_FILE" 2>&1 &

      newpid="$!"
      echo $newpid > "$MIXSERV_PID_FILE"
      sleep 1

      # Checks if the process has died
      if [[ ! $(ps -p "$newpid" -o comm=) =~ "java" ]]; then
        echo "failed to launch a MIX server"
      fi
    else
      echo "executable jar ${JARFILE} not found"
      exit 1
    fi
    ;;

  (stop)

    if [ -f $MIXSERV_PID_FILE ]; then
      TARGET_ID="$(cat $MIXSERV_PID_FILE)"
      if [[ $(ps -p "$TARGET_ID" -o comm=) =~ "java" ]]; then
        echo "stopping a MIX server (PID=${TARGET_ID})" 
        kill "$TARGET_ID" && rm -f "$MIXSERV_PID_FILE"
      else
        echo "no MIX server to stop"
      fi
    else
      echo "no MIX server to stop"      
    fi
    ;;

  (status)

    if [ -f $MIXSERV_PID_FILE ]; then
      TARGET_ID="$(cat $MIXSERV_PID_FILE)"
      if [[ $(ps -p "$TARGET_ID" -o comm=) =~ "java" ]]; then
        echo "MIX server is running (PID=${TARGET_ID})"
        exit 0
      else
        echo "file ${MIXSERV_PID_FILE} is present but MIX server is not running"
        exit 1
      fi
    else
      echo "MIX server is not running"
      exit 2
    fi
    ;;

  (*)
    echo $usage
    exit 1
    ;;

esac

