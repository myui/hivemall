#!/bin/sh

JMXOPTS="-Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.port=9010 -Dcom.sun.management.jmxremote.local.only=false -Dcom.sun.management.jmxremote.authenticate=false"

VMOPTS="-Xmx4g -da -server -XX:+PrintGCDetails -XX:+UseNUMA -XX:+UseParallelGC $VMOPTS"
# VMOPTS="-Xmx4g -da -server -XX:+PrintGCDetails -XX:+UseNUMA -XX:+UseParallelGC $JMXOPTS $VMOPTS"

java ${VMOPTS} -jar hivemall-fat.jar $@
