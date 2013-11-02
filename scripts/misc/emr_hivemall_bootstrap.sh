#!/bin/bash

mkdir tmp
wget --no-check-certificate -P ./tmp \
 https://github.com/myui/hivemall/raw/master/target/hivemall.jar https://github.com/myui/hivemall/raw/master/scripts/ddl/define-all.hive
