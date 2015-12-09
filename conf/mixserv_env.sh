#!/bin/sh

MIXSERV_JMXOPTS+="-Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.port=9010 -Dcom.sun.management.jmxremote.local.only=false -Dcom.sun.management.jmxremote.authenticate=false"
MIXSERV_VMOPTS+="-Xmx4g -da -server -XX:+PrintGCDetails -XX:+UseNUMA -XX:+UseParallelGC"
MIXSERV_OPS+="-sync 30"
MIXSERV_NICENESS="0"

# Settings for a YARN mode
MIXSERV_YARN_ENABLED="1"
MIXSERV_YARN_TIMELINE_ENABLED="0"
MIXSERV_YARN_CONTAINER_NUM="1"
MIXSERV_YARN_CONTAINER_VCORES="1"
MIXSERV_YARN_CONTAINER_MEM="128"

