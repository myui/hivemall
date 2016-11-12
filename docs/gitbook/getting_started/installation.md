<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->
        
Prerequisites
============

* Hive v0.12 or later
* Java 7 or later
* [hivemall-core-xxx-with-dependencies.jar](https://github.com/myui/hivemall/releases)
* [define-all.hive](https://github.com/myui/hivemall/releases)

Installation
============

Add the following two lines to your `$HOME/.hiverc` file.

```
add jar /home/myui/tmp/hivemall-core-xxx-with-dependencies.jar;
source /home/myui/tmp/define-all.hive;
```

This automatically loads all Hivemall functions every time you start a Hive session. Alternatively, you can run the following command each time.

```
$ hive
add jar /tmp/hivemall-core-xxx-with-dependencies.jar;
source /tmp/define-all.hive;
```