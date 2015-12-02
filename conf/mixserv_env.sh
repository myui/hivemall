#!/bin/sh

MIXSERV_JMXOPTS+="-Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.port=9010 -Dcom.sun.management.jmxremote.local.only=false -Dcom.sun.management.jmxremote.authenticate=false"
MIXSERV_VMOPTS+="-Xmx4g -da -server -XX:+PrintGCDetails -XX:+UseNUMA -XX:+UseParallelGC"
MIXSERV_OPS+="-sync 30"
MIXSERV_NICENESS="0"

