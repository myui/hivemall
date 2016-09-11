#!/bin/bash

# Hivemall: Hive scalable Machine Learning Library
#
# Copyright (C) 2015 Makoto YUI
# Copyright (C) 2013-2015 National Institute of Advanced Industrial Science and Technology (AIST)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -eu
set -o pipefail

# Target commit hash value
XGBOOST_HASHVAL='7ab15a0b31c870c7779691639f521df3ccd4a56e'

# Move to a top directory
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

# Final output dir for a custom-compiled xgboost binary
HIVEMALL_LIB_DIR="$HIVEMALL_HOME/xgboost/src/main/resources/lib/"
rm -rf $HIVEMALL_LIB_DIR >> /dev/null
mkdir -p $HIVEMALL_LIB_DIR

# Move to an output directory
XGBOOST_OUT="$HIVEMALL_HOME/target/xgboost-$XGBOOST_HASHVAL"
rm -rf $XGBOOST_OUT >> /dev/null
mkdir -p $XGBOOST_OUT
cd $XGBOOST_OUT

# Fetch xgboost sources
git clone --progress https://github.com/maropu/xgboost.git
cd xgboost
git checkout $XGBOOST_HASHVAL

# Resolve dependent sources
git submodule init
git submodule update

# Copy a built binary to the output
cd jvm-packages
ENABLE_STATIC_LINKS=1 ./create_jni.sh
cp ./lib/libxgboost4j.* "$HIVEMALL_LIB_DIR"

