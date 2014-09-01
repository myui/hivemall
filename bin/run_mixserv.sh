#!/bin/sh

VMOPTS="-Xmx4g -da -server $VMOPTS"

java ${VMOPTS} -jar hivemall-all.jar $@
