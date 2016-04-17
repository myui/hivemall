#!/usr/bin/env bash

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Copyed from commit 48682f6bf663e54cb63b7e95a4520d34b6fa890b in Apache Spark

# Determine the current working directory
_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
# Preserve the calling directory
_CALLING_DIR="$(pwd)"

# Installs any application tarball given a URL, the expected tarball name,
# and, optionally, a checkable binary path to determine if the binary has
# already been installed
## Arg1 - URL
## Arg2 - Tarball Name
## Arg3 - Checkable Binary
install_app() {
  local remote_tarball="$1/$2"
  local local_tarball="${_DIR}/$2"
  local binary="${_DIR}/$3"
  local curl_opts="--progress-bar -L"
  local wget_opts="--progress=bar:force ${wget_opts}"

  if [ -z "$3" -o ! -f "$binary" ]; then
    # check if we already have the tarball
    # check if we have curl installed
    # download application
    [ ! -f "${local_tarball}" ] && [ $(command -v curl) ] && \
      echo "exec: curl ${curl_opts} ${remote_tarball}" 1>&2 && \
      curl ${curl_opts} "${remote_tarball}" > "${local_tarball}"
    # if the file still doesn't exist, lets try `wget` and cross our fingers
    [ ! -f "${local_tarball}" ] && [ $(command -v wget) ] && \
      echo "exec: wget ${wget_opts} ${remote_tarball}" 1>&2 && \
      wget ${wget_opts} -O "${local_tarball}" "${remote_tarball}"
    # if both were unsuccessful, exit
    [ ! -f "${local_tarball}" ] && \
      echo -n "ERROR: Cannot download $2 with cURL or wget; " && \
      echo "please install manually and try again." && \
      exit 2
    cd "${_DIR}" && tar -xzf "$2"
    rm -rf "$local_tarball"
  fi
}

# Install maven under the bin/ folder
install_mvn() {
  local MVN_VERSION="3.3.9"

  install_app \
    "http://archive.apache.org/dist/maven/maven-3/${MVN_VERSION}/binaries" \
    "apache-maven-${MVN_VERSION}-bin.tar.gz" \
    "apache-maven-${MVN_VERSION}/bin/mvn"

  MVN_BIN="${_DIR}/apache-maven-${MVN_VERSION}/bin/mvn"
}

# Check for the `--force` flag dictating that `mvn` should be downloaded
# regardless of whether the system already has a `mvn` install
if [ "$1" == "--force" ]; then
  FORCE_MVN=1
  shift
fi

# Install Maven if necessary
MVN_BIN="$(command -v mvn)"

if [ ! "$MVN_BIN" -o -n "$FORCE_MVN" ]; then
  install_mvn
fi

# Reset the current working directory
cd "${_CALLING_DIR}"

echo "Using \`mvn\` from path: $MVN_BIN" 1>&2

# Last, call the `mvn` command as usual
${MVN_BIN} "$@"
