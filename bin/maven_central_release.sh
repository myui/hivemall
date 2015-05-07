#!/bin/sh

cd ..
mvn clean deploy -DperformRelease=true -Dskiptests=true -Dmaven.test.skip=true
