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
        
Hive v0.13 or later supports [permanent functions](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL#LanguageManualDDL-Create/DropFunction) that live across sessions.

Permanent functions are useful when you are using Hive through Hiveserver or to avoid hivemall installation for each session.

_Note: This feature is supported since hivemall-0.3 beta 3 or later._

<!-- toc -->

# Put hivemall jar to HDFS

First, put hivemall jar to HDFS as follows:
```sh
hadoop fs -mkdir -p /apps/hivemall
hadoop fs -put hivemall-with-dependencies.jar /apps/hivemall
```

# Create permanent functions

_The following is an auxiliary step to define functions for hivemall databases, not for the default database._
```sql
CREATE DATABASE IF NOT EXISTS hivemall;
USE hivemall;
```

Then, create permanent functions using [define-all-as-permanent.hive](https://github.com/myui/hivemall/blob/master/resources/ddl/define-all-as-permanent.hive), a DDL script to define permanent UDFs.
```sql
set hivevar:hivemall_jar=hdfs:///apps/hivemall/hivemall-with-dependencies.jar;

source /tmp/define-all-as-permanent.hive;
```

# Confirm functions

```sql
show functions "hivemall.*";

> hivemall.adadelta
> hivemall.adagrad
```

> #### Caution
You need to specify "hivemall." prefix to call hivemall UDFs in your queries if UDFs are loaded into non-default scheme, in this case _hivemall_.