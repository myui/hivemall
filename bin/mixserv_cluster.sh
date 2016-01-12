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

# Start MIX server instances on each machine specified
# in the conf/MIXSERV_LIST file

usage="Usage: mixserv_cluster.sh (start|stop|status)"

if [ "$HIVEMALL_HOME" == "" ]; then
  echo "env HIVEMALL_HOME not defined"
  exit 1
fi

cmd=$1
case $cmd in
  (start)
    ;;
  (stop)
    ;;
  (status)
    ;;
  (*)
    echo $usage
    exit 1
    ;;
esac

# Check if YARN or not
if [ "$MIXSERV_YARN_ENABLED" == "1" ]; then
  /bin/sh "$HIVEMALL_HOME/bin/mixserv_daemon.sh" $cmd
else
  MIXSERV_HOSTS="$HIVEMALL_HOME/conf/MIXSERV_LIST"
  MIXSERV_SSH_OPTS="-o StrictHostKeyChecking=no"

  # Load host entries from the servers file
  if [ -f "$MIXSERV_HOSTS" ]; then
    HOSTLIST=`cat $MIXSERV_HOSTS`
  else
    HOSTLIST=localhost
  fi

  for slave in `echo "$HOSTLIST" | sed  "s/#.*$//;/^$/d"`; do
    ssh $MIXSERV_SSH_OPTS "$slave" "$HIVEMALL_HOME/bin/mixserv_daemon.sh" $cmd 2>&1 | sed "s/^/$slave: /" &
  done

  wait
fi

