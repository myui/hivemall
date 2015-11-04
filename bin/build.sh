#!/bin/sh

cd ..
mvn clean package -Dskiptests=true -Dmaven.test.skip=true
