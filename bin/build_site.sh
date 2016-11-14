#!/bin/sh
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

if [ "$HIVEMALL_HOME" == "" ]; then
    if [ -e ../bin/${0##*/} ]; then
	HIVEMALL_HOME=".."
    elif [ -e ./bin/${0##*/} ]; then
	HIVEMALL_HOME="."
    else
	echo "env HIVEMALL_HOME not defined"
	exit 1
    fi
fi

cd $HIVEMALL_HOME
HIVEMALL_HOME=`pwd`

##
# Run maven-site
##

mvn clean site

##
# building gitbook userguide
##

if ! [ -x "$(command -v gitbook)" ]; then
  echo "gitbook is not installed .." >&2
  echo "Run 'npm install gitbook-cli -g' to install gitbook" >&2
  exit 1
fi

cd ${HIVEMALL_HOME}/docs/gitbook
gitbook install && gitbook build
cd $HIVEMALL_HOME

cp -R docs/gitbook/_book target/site/userguide

##
# Run HTTP server on localhost
##

# ruby
cd ${HIVEMALL_HOME}/target/site
ruby -rwebrick -e 'WEBrick::HTTPServer.new(:DocumentRoot => "./", :Port => 8000).start'

# python3
#python -m http.server 8000

# python2
#python -m SimpleHTTPServer 8000



