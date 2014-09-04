#!/bin/sh

VMOPTS="-Xmx4g -da -server -XX:+PrintGCDetails -XX:+UseNUMA -XX:+UseParallelGC $VMOPTS"

java ${VMOPTS} -jar hivemall-fat.jar $@
