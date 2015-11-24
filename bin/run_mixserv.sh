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

VERSION_FILE="$HIVEMALL_HOME/VERSION"

# Load a version number from a VERSION file
if [ -f "$VERSION_FILE" ]; then
  VERSION=`cat "$VERSION_FILE"`
  JARFILE="$HIVEMALL_HOME/target/hivemall-mixserv-$VERSION-fat.jar"
else
  JARFILE="hivemall-mixserv.jar"
fi

JMXOPTS="-Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.port=9010 -Dcom.sun.management.jmxremote.local.only=false -Dcom.sun.management.jmxremote.authenticate=false"

VMOPTS="-Xmx4g -da -server -XX:+PrintGCDetails -XX:+UseNUMA -XX:+UseParallelGC $VMOPTS"
# VMOPTS="-Xmx4g -da -server -XX:+PrintGCDetails -XX:+UseNUMA -XX:+UseParallelGC $JMXOPTS $VMOPTS"

java ${VMOPTS} -jar ${JARFILE} $@
