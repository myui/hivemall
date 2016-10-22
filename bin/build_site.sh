#!/bin/sh

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
mvn clean site

cp -R docs/gitbook/_book target/site/userguide

#
# Run HTTP server on localhost
#

# ruby
cd $HIVEMALL_HOME/target/site
ruby -rwebrick -e 'WEBrick::HTTPServer.new(:DocumentRoot => "./", :Port => 8000).start'

# python3
#python -m http.server 8000

# python2
#python -m SimpleHTTPServer 8000



