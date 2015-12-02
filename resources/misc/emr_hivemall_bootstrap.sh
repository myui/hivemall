#!/bin/bash

mkdir -p /home/hadoop/tmp
wget --no-check-certificate -P /home/hadoop/tmp \
 https://github.com/myui/hivemall/raw/master/target/hivemall-with-dependencies.jar https://github.com/myui/hivemall/raw/master/scripts/ddl/define-all.hive
