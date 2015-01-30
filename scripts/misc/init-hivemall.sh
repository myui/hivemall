#!/usr/bin/sh

echo "Removing existing hivemall jar"
hive -e "delete jar hivemall-with-dependencies.jar"
echo "Adding hivemall jar"
hive -e "add jar hivemall-with-dependencies.jar"
echo "Loading hivemall ddl"
hive -e "source define-all.hive;"
echo "Succceded to load ddl"
